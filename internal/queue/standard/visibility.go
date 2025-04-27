package standard

import (
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// ChangeMessageVisibility changes the visibility timeout of a message
func (q *queue) ChangeMessageVisibility(receiptHandle string, timeout time.Duration) bool {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// Validate and adjust timeout
	if timeout < 0 {
		timeout = 0
	}

	msg, found := q.MessageStore.GetMessageByReceipt(receiptHandle)
	if !found || !msg.IsInFlight() {
		return false
	}

	// Update visibility
	if timeout == 0 {
		// Make visible immediately
		success := q.MessageStore.ResetVisibility(receiptHandle)
		if success {
			// Signal that messages are available
			q.Cond.Signal()
		}
		return success
	} else {
		// Set new timeout
		msg.InvisibleUntil = time.Now().Add(timeout)
		return true
	}
}

// ChangeMessageVisibilityBatch changes the visibility timeout of multiple messages
func (q *queue) ChangeMessageVisibilityBatch(items []common.ChangeMessageVisibilityBatchItem) []common.BatchResult {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	results := make([]common.BatchResult, len(items))
	for i := range items {
		results[i] = common.BatchResult{
			Success:       false,
			ReceiptHandle: items[i].ReceiptHandle,
		}
	}

	// Track if we need to signal for visible messages
	needsSignal := false

	for i, item := range items {
		if item.ReceiptHandle == "" {
			continue
		}

		// Validate and adjust timeout
		timeout := item.Timeout
		if timeout < 0 {
			timeout = 0
		}

		msg, found := q.MessageStore.GetMessageByReceipt(item.ReceiptHandle)
		if !found || !msg.IsInFlight() {
			continue
		}

		// Update visibility
		if timeout == 0 {
			// Make visible immediately
			if q.MessageStore.ResetVisibility(item.ReceiptHandle) {
				results[i].Success = true
				results[i].MessageID = msg.Attributes.MessageID
				needsSignal = true
			}
		} else {
			// Set new timeout
			msg.InvisibleUntil = time.Now().Add(timeout)
			results[i].Success = true
			results[i].MessageID = msg.Attributes.MessageID
		}
	}

	// Signal if any messages became visible
	if needsSignal {
		q.Cond.Signal()
	}

	return results
}

// CheckVisibilityTimeouts checks for and resets messages whose visibility timeout has expired
func (q *queue) CheckVisibilityTimeouts() {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	now := time.Now()
	hasVisible := false

	// Scan in-flight messages for visibility timeouts
	for receiptHandle, msg := range q.MessageStore.(*messageStore).inFlightMessages {
		if !msg.IsInFlight() {
			continue
		}
		if !now.After(msg.InvisibleUntil) || msg.IsDelayed() {
			continue
		}
		// Reset visibility through the message store
		if q.MessageStore.ResetVisibility(receiptHandle) {
			hasVisible = true
		}
	}

	// Signal that messages are available if any became visible
	if hasVisible {
		q.Cond.Signal()
	}
}

// StartVisibilityChecker starts a background goroutine to check for expired visibility timeouts
func (q *queue) StartVisibilityChecker() {
	ticker := time.NewTicker(common.DefaultVisibilityCheckInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				q.CheckVisibilityTimeouts()
			case <-q.Shutdown:
				return
			}
		}
	}()
}

// ForceCheckVisibilityTimeouts manually triggers a check for expired visibility timeouts.
// This is primarily used for testing.
func (q *queue) ForceCheckVisibilityTimeouts() {
	q.CheckVisibilityTimeouts()
}
