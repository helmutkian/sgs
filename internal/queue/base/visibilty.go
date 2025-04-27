package base

import (
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// VisibilityManager contains methods related to message visibility
type VisibilityManager[I any] interface {
	// Called when visibility changes to zero (become visible)
	OnMessageBecameVisible(msg *common.MessageWithVisibility)
}

// waitForVisibleMessages blocks until a visible message is available
func (q *BaseQueue[I]) WaitForVisibleMessages(hasQueueLock bool) bool {
	if !hasQueueLock {
		q.Mu.Lock()
		defer q.Mu.Unlock()
	}

	// Check for visible messages immediately first
	if q.HasVisibleMessages() {
		return true
	}

	// Create a timeout channel
	timeout := time.After(100 * time.Millisecond)
	timeoutTriggered := false

	for !q.HasVisibleMessages() {
		// Release the lock and prepare for conditional wait with timeout
		waitChan := make(chan struct{})
		go func() {
			// This goroutine will wait on the condition and signal when woken
			q.Mu.Lock()
			q.Cond.Wait()
			q.Mu.Unlock()
			close(waitChan)
		}()

		// Temporarily release the lock during the wait
		q.Mu.Unlock()

		// Wait for either condition signal, timeout, or shutdown
		select {
		case <-waitChan:
			// Condition was signaled
		case <-timeout:
			// Timeout occurred
			timeoutTriggered = true
		case <-q.Shutdown:
			// Shutdown signal
			// Re-acquire the lock before returning
			q.Mu.Lock()
			return false
		}

		// Re-acquire the lock
		q.Mu.Lock()

		// Check for shutdown again after waking up
		select {
		case <-q.Shutdown:
			return false
		default:
			// Proceed
		}

		// If we timed out, return false to indicate no messages are available
		if timeoutTriggered {
			return false
		}
	}

	return true
}

// HasVisibleMessages checks if there are any visible messages
func (q *BaseQueue[I]) HasVisibleMessages() bool {
	for _, msg := range q.MessageStore.Messages() {
		if msg.IsVisible() {
			return true
		}
	}
	return false
}

func (q *BaseQueue[I]) changeMessageVisibility(receiptHandle string, timeout time.Duration) (*common.MessageWithVisibility, bool) {
	if timeout < 0 {
		timeout = 0
	}

	msg, found := q.MessageStore.GetMessageByReceipt(receiptHandle)
	if !found || !msg.IsInFlight() {
		return nil, false
	}

	// Update visibility
	if timeout == 0 {
		// Make visible immediately - this should be handled by the message store
		success := q.MessageStore.ResetVisibility(receiptHandle)
		if success {
			// Signal that messages are available
			q.Cond.Signal()
			return msg, true
		}
		return nil, false
	} else {
		// Set new timeout
		msg.InvisibleUntil = time.Now().Add(timeout)
		return msg, true
	}
}

func (q *BaseQueue[I]) ChangeMessageVisibilityBatch(items []common.ChangeMessageVisibilityBatchItem) []common.BatchResult {
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

		msg, success := q.changeMessageVisibility(item.ReceiptHandle, item.Timeout)
		if success {
			results[i].Success = true
			results[i].MessageID = msg.Attributes.MessageID

			// If visibility was reset to 0, signal is needed
			if item.Timeout <= 0 {
				needsSignal = true
			}
		}
	}

	// Signal if any messages became visible
	if needsSignal {
		q.Cond.Signal()
	}

	return results
}

// CheckVisibilityTimeouts checks for and resets messages whose visibility timeout has expired
func (q *BaseQueue[I]) CheckVisibilityTimeouts() {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	now := time.Now()
	signalNeeded := false

	for _, msg := range q.MessageStore.Messages() {
		// If the message is in flight and its visibility timeout has expired
		if msg.IsInFlight() && now.After(msg.InvisibleUntil) && msg.DelayedUntil.IsZero() {
			// Reset visibility through the message store if receipt handle exists
			if msg.ReceiptHandle != "" {
				if q.MessageStore.ResetVisibility(msg.ReceiptHandle) {
					signalNeeded = true
				}
			}
		}
	}

	// Signal that messages are available if any became visible
	if signalNeeded {
		q.Cond.Signal()
	}
}

// StartVisibilityChecker starts a background goroutine to check for expired visibility timeouts
func (q *BaseQueue[I]) StartVisibilityChecker() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
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
