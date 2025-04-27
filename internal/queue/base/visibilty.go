package base

import (
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

const (
	DefaultVisibilityCheckInterval = common.DefaultVisibilityCheckInterval
)

// WaitForVisibleMessages blocks until a visible message is available
// Takes a function parameter to check for visible messages since that implementation
// differs between queue types
func (q *BaseQueue[I]) WaitForVisibleMessages(hasVisibleMessages func() bool) bool {
	// Check for visible messages immediately first
	if hasVisibleMessages() {
		return true
	}

	// Create a timeout channel
	timeout := time.After(100 * time.Millisecond)
	timeoutTriggered := false

	for !hasVisibleMessages() {
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
