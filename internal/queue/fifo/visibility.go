package fifo

import (
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// ChangeMessageVisibility changes the visibility timeout of a message
// and handles FIFO-specific behavior for message group locks
func (q *Queue) ChangeMessageVisibility(receiptHandle string, timeout time.Duration) bool {
	q.Mu.Lock()

	// Find the message with this receipt handle using the new message store method
	msg, found := q.MessageStore.(*messageStore).GetMessageByReceipt(receiptHandle)
	if !found || !msg.IsInFlight() {
		q.Mu.Unlock()
		return false
	}

	targetGroupID := msg.Attributes.MessageGroupID

	// Update the visibility timeout
	if timeout <= 0 {
		// Reset visibility makes the message immediately visible again
		success := q.MessageStore.(*messageStore).ResetVisibility(receiptHandle)
		q.Mu.Unlock()

		if !success || q.HighThroughputMode {
			return success
		}

		// In standard FIFO mode, release the message group lock
		q.MessageGroupMutex.Lock()
		delete(q.MessageGroupLocks, targetGroupID)
		q.MessageGroupMutex.Unlock()

		// Signal that a message is available
		q.Cond.Signal()
		return success
	} else {
		// Just update the visibility timeout
		msg.InvisibleUntil = time.Now().Add(timeout)
		q.Mu.Unlock()
		return true
	}
}

// ChangeMessageVisibilityBatch changes the visibility timeout of multiple messages
func (q *Queue) ChangeMessageVisibilityBatch(items []common.ChangeMessageVisibilityBatchItem) []common.BatchResult {
	results := make([]common.BatchResult, len(items))
	for i := range results {
		results[i] = common.BatchResult{
			Success:       false,
			ReceiptHandle: items[i].ReceiptHandle,
		}
	}

	// Track which message groups need to be unlocked (for messages becoming visible again)
	groupsToUnlock := make(map[string]bool)

	q.Mu.Lock()
	for i, item := range items {
		if item.ReceiptHandle == "" {
			continue
		}

		// Get the message using the new message store method
		msg, found := q.MessageStore.(*messageStore).GetMessageByReceipt(item.ReceiptHandle)
		if !found || !msg.IsInFlight() {
			continue
		}

		if item.Timeout <= 0 {
			// Reset visibility makes the message immediately visible again
			if q.MessageStore.(*messageStore).ResetVisibility(item.ReceiptHandle) {
				results[i].Success = true
				results[i].MessageID = msg.Attributes.MessageID

				// In standard FIFO mode, mark this group for unlocking
				if !q.HighThroughputMode {
					groupsToUnlock[msg.Attributes.MessageGroupID] = true
				}
			}
		} else {
			// Just update the visibility timeout
			msg.InvisibleUntil = time.Now().Add(item.Timeout)
			results[i].Success = true
			results[i].MessageID = msg.Attributes.MessageID
		}
	}
	q.Mu.Unlock()

	// Release message group locks if needed
	if !q.HighThroughputMode && len(groupsToUnlock) > 0 {
		q.MessageGroupMutex.Lock()
		for groupID := range groupsToUnlock {
			delete(q.MessageGroupLocks, groupID)
		}
		q.MessageGroupMutex.Unlock()

		// Signal that messages might be available
		q.Cond.Signal()
	}

	return results
}
