package standard

import (
	"iter"
	"sync"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// messageStore implements a two-part data structure for message storage:
// 1. pendingMessages: Messages that are visible and waiting to be received (no receipt handles)
// 2. inFlightMessages: Messages that have been received and have receipt handles
type messageStore struct {
	pendingMessages  []*common.MessageWithVisibility
	inFlightMessages map[string]*common.MessageWithVisibility // keyed by receipt handle
	mu               sync.RWMutex
}

var _ common.MessageStore[int] = &messageStore{}

func newMessageStore() *messageStore {
	return &messageStore{
		pendingMessages:  make([]*common.MessageWithVisibility, 0),
		inFlightMessages: make(map[string]*common.MessageWithVisibility),
	}
}

// Messages iterates through all messages (both pending and in-flight)
func (ms *messageStore) Messages() iter.Seq2[int, *common.MessageWithVisibility] {
	return func(yield func(int, *common.MessageWithVisibility) bool) {
		ms.mu.RLock()
		defer ms.mu.RUnlock()

		// First yield all pending messages with their array indices
		for i, msg := range ms.pendingMessages {
			if !yield(i, msg) {
				return
			}
		}

		// For in-flight messages, continue with negative indices
		// This is a convention to distinguish them from pending messages
		// The actual indices don't matter as they'll be looked up by receipt handle
		i := -1
		for _, msg := range ms.inFlightMessages {
			if !yield(i, msg) {
				return
			}
			i--
		}
	}
}

// Message returns a message by its index
func (ms *messageStore) Message(index int) (*common.MessageWithVisibility, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if index >= 0 && index < len(ms.pendingMessages) {
		return ms.pendingMessages[index], true
	}

	// For negative indices, this function is mainly used for compatibility
	// In practice, we'll look up in-flight messages by receipt handle
	return nil, false
}

// RemoveMessage removes a message by index or receipt handle (encoded in index)
func (ms *messageStore) RemoveMessage(index int) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if index >= 0 {
		// Removing from pending messages array
		if index < len(ms.pendingMessages) {
			// Remove the message from pending array
			ms.pendingMessages = append(ms.pendingMessages[:index], ms.pendingMessages[index+1:]...)
			return true
		}
		return false
	} else {
		// Negative index means we need to look through in-flight messages
		// This shouldn't normally happen but is handled for compatibility
		count := 0
		for _, msg := range ms.inFlightMessages {
			if count == -index-1 { // Convert negative index to position
				if msg.ReceiptHandle != "" {
					delete(ms.inFlightMessages, msg.ReceiptHandle)
					return true
				}
				return false
			}
			count++
		}
		return false
	}
}

// RemoveMessageByReceipt removes a message by its receipt handle
// This is the preferred way to remove in-flight messages
func (ms *messageStore) RemoveMessageByReceipt(receiptHandle string) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.inFlightMessages[receiptHandle]; exists {
		delete(ms.inFlightMessages, receiptHandle)
		return true
	}
	return false
}

// AddMessages adds new messages to the pending queue
func (ms *messageStore) AddMessages(msgs ...*common.MessageWithVisibility) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	for _, msg := range msgs {
		if msg.Status == common.MessageInFlight && msg.ReceiptHandle != "" {
			// If it already has a receipt handle, it goes to in-flight messages
			ms.inFlightMessages[msg.ReceiptHandle] = msg
		} else {
			// Otherwise it goes to pending messages
			// Clear receipt handle and ensure status is visible
			msg.ReceiptHandle = ""
			msg.Status = common.MessageVisible
			ms.pendingMessages = append(ms.pendingMessages, msg)
		}
	}
}

// TotalMessages returns the total number of messages in the store
func (ms *messageStore) TotalMessages() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	return len(ms.pendingMessages) + len(ms.inFlightMessages)
}

// MarkMessageInFlight moves a message from pending to in-flight
func (ms *messageStore) MarkMessageInFlight(index int, receiptHandle string, visibilityTimeout time.Duration) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if index < 0 || index >= len(ms.pendingMessages) {
		return false
	}

	msg := ms.pendingMessages[index]
	msg.Status = common.MessageInFlight
	msg.ReceiptHandle = receiptHandle
	msg.InvisibleUntil = time.Now().Add(visibilityTimeout)
	msg.ReceiveCount++

	// Move from pending to in-flight
	ms.inFlightMessages[receiptHandle] = msg
	ms.pendingMessages = append(ms.pendingMessages[:index], ms.pendingMessages[index+1:]...)

	return true
}

// GetVisibleMessage finds the first visible message in the pending queue
func (ms *messageStore) GetVisibleMessage() (int, *common.MessageWithVisibility, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	for i, msg := range ms.pendingMessages {
		if msg.IsVisible() && !msg.IsDelayed() {
			return i, msg, true
		}
	}
	return -1, nil, false
}

// ResetVisibility makes a message visible again by moving it from in-flight
// back to the pending messages collection
func (ms *messageStore) ResetVisibility(receiptHandle string) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Find the message in the in-flight collection
	msg, exists := ms.inFlightMessages[receiptHandle]
	if !exists {
		return false
	}

	// Make a copy of the message, reset its receipt handle and visibility
	msgCopy := *msg
	msgCopy.ReceiptHandle = ""
	msgCopy.InvisibleUntil = time.Time{}
	msgCopy.Status = common.MessageVisible

	// Add to pending messages
	ms.pendingMessages = append(ms.pendingMessages, &msgCopy)

	// Remove from in-flight messages
	delete(ms.inFlightMessages, receiptHandle)

	return true
}

// GetMessageByReceipt returns the message associated with the given receipt handle
func (ms *messageStore) GetMessageByReceipt(receiptHandle string) (*common.MessageWithVisibility, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// First check in-flight messages, as this is where messages with receipt handles should be
	if msg, ok := ms.inFlightMessages[receiptHandle]; ok {
		return msg, true
	}

	// Should not normally find receipt handles in pending messages,
	// but check for completeness
	for _, msg := range ms.pendingMessages {
		if msg.ReceiptHandle == receiptHandle {
			return msg, true
		}
	}

	return nil, false
}

// GetMessageCount returns counts of visible and in-flight messages
func (ms *messageStore) GetMessageCount() (visible int, inFlight int) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	visible = 0
	for _, msg := range ms.pendingMessages {
		if msg.IsVisible() && !msg.IsDelayed() {
			visible++
		}
	}

	inFlight = len(ms.inFlightMessages)
	return
}
