package fifo

import (
	"github.com/helmutkian/sgs/internal/queue/common"
)

// DeleteMessage removes a message from the queue and handles FIFO-specific behavior for message group locks
func (q *Queue) DeleteMessage(receiptHandle string) bool {
	// Find the message first to get its group ID before deleting it
	var targetGroupID string
	found := false

	q.Mu.Lock()
	// Try to get directly from the messageStore
	msg, exists := q.MessageStore.(*messageStore).GetMessageByReceipt(receiptHandle)
	if exists && msg.IsInFlight() {
		targetGroupID = msg.Attributes.MessageGroupID
		found = true
	}
	q.Mu.Unlock()

	if !found {
		return false
	}

	// Use the direct receipt handle method if available
	q.Mu.Lock()
	deleted := q.MessageStore.(*messageStore).RemoveMessageByReceipt(receiptHandle)
	q.Mu.Unlock()

	if deleted && !q.HighThroughputMode {
		// Release the message group lock if needed
		q.MessageGroupMutex.Lock()
		delete(q.MessageGroupLocks, targetGroupID)
		q.MessageGroupMutex.Unlock()
	}

	return deleted
}

// DeleteMessageBatch deletes multiple messages at once
func (q *Queue) DeleteMessageBatch(receiptHandles []string) []common.BatchResult {
	// Initialize results
	results := make([]common.BatchResult, len(receiptHandles))
	for i := range results {
		results[i] = common.BatchResult{
			Success:       false,
			ReceiptHandle: receiptHandles[i],
		}
	}

	// Map to track message groups that need unlocking
	messageGroupsToUnlock := make(map[string]bool)

	q.Mu.Lock()

	// Process each receipt handle
	for i, receiptHandle := range receiptHandles {
		if receiptHandle == "" {
			continue
		}

		// Get the message to capture ID and group info
		msg, found := q.MessageStore.(*messageStore).GetMessageByReceipt(receiptHandle)
		if !found || !msg.IsInFlight() {
			continue
		}

		// Remove message by receipt handle
		if q.MessageStore.(*messageStore).RemoveMessageByReceipt(receiptHandle) {
			results[i].Success = true
			results[i].MessageID = msg.Attributes.MessageID

			// Track group for unlocking
			if !q.HighThroughputMode {
				messageGroupsToUnlock[msg.Attributes.MessageGroupID] = true
			}
		}
	}

	q.Mu.Unlock()

	// Release message group locks if needed
	if !q.HighThroughputMode && len(messageGroupsToUnlock) > 0 {
		q.MessageGroupMutex.Lock()
		for groupID := range messageGroupsToUnlock {
			delete(q.MessageGroupLocks, groupID)
		}
		q.MessageGroupMutex.Unlock()
	}

	return results
}
