package fifo

import (
	"iter"
	"slices"
	"sync"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// messageStore implements a two-part data structure for FIFO message storage:
// 1. pendingMessages: Map of group ID to arrays of visible pending messages
// 2. inFlightMessages: Map of receipt handles to in-flight messages
type messageStore struct {
	// Map of group ID to arrays of messages (for FIFO ordering)
	pendingMessages map[string][]*common.MessageWithVisibility
	// Map of receipt handle to message (for quick lookup)
	inFlightMessages map[string]*common.MessageWithVisibility
	// Additional index to find which group a message belongs to
	receiptToGroup map[string]string
	mu             sync.RWMutex
}

var _ common.MessageStore[fifoMessageIndex] = &messageStore{}

func newMessageStore() *messageStore {
	return &messageStore{
		pendingMessages:  make(map[string][]*common.MessageWithVisibility),
		inFlightMessages: make(map[string]*common.MessageWithVisibility),
		receiptToGroup:   make(map[string]string),
	}
}

type fifoMessageIndex struct {
	groupID string
	pos     int
}

// Messages iterates through all messages (both pending and in-flight)
func (ms *messageStore) Messages() iter.Seq2[fifoMessageIndex, *common.MessageWithVisibility] {
	return func(yield func(fifoMessageIndex, *common.MessageWithVisibility) bool) {
		ms.mu.RLock()
		defer ms.mu.RUnlock()

		// First yield all pending messages with their array indices
		for groupID, msgs := range ms.pendingMessages {
			for i, msg := range msgs {
				ix := fifoMessageIndex{
					groupID: groupID,
					pos:     i,
				}
				if !yield(ix, msg) {
					return
				}
			}
		}

		// Then yield all in-flight messages with negative indices
		// The actual indices don't matter much since they'll be looked up by receipt handle
		for receipt, msg := range ms.inFlightMessages {
			groupID := ms.receiptToGroup[receipt]
			ix := fifoMessageIndex{
				groupID: groupID,
				pos:     -1, // Use -1 to indicate it's from inFlightMessages
			}
			if !yield(ix, msg) {
				return
			}
		}
	}
}

// Message returns a message by its index
func (ms *messageStore) Message(ix fifoMessageIndex) (*common.MessageWithVisibility, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ix.pos >= 0 {
		// Positive index means it's in the pending messages
		msgs, ok := ms.pendingMessages[ix.groupID]
		if !ok || ix.pos >= len(msgs) {
			return nil, false
		}
		return msgs[ix.pos], true
	}

	// For negative indices, this function is mainly used for compatibility
	// In practice, we'll look up in-flight messages by receipt handle
	return nil, false
}

// TotalMessages returns the total number of messages in the store
func (ms *messageStore) TotalMessages() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	total := 0
	for _, msgs := range ms.pendingMessages {
		total += len(msgs)
	}
	total += len(ms.inFlightMessages)
	return total
}

// RemoveMessage removes a message by index
func (ms *messageStore) RemoveMessage(ix fifoMessageIndex) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ix.pos >= 0 {
		// Removing from pending messages array
		msgs, ok := ms.pendingMessages[ix.groupID]
		if !ok || ix.pos >= len(msgs) {
			return false
		}

		// Remove the message from pending array
		ms.pendingMessages[ix.groupID] = slices.Delete(msgs, ix.pos, ix.pos+1)

		// Clean up empty group if needed
		if len(ms.pendingMessages[ix.groupID]) == 0 {
			delete(ms.pendingMessages, ix.groupID)
		}

		return true
	} else {
		// This is a legacy path that shouldn't be used much
		// For in-flight messages, use RemoveMessageByReceipt instead
		return false
	}
}

// AddMessages adds new messages to the appropriate stores
func (ms *messageStore) AddMessages(msgs ...*common.MessageWithVisibility) {
	if len(msgs) == 0 {
		return
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Process each message
	for _, msg := range msgs {
		groupID := msg.Attributes.MessageGroupID

		if msg.Status == common.MessageInFlight && msg.ReceiptHandle != "" {
			// If it's in-flight, store by receipt handle
			ms.inFlightMessages[msg.ReceiptHandle] = msg
			ms.receiptToGroup[msg.ReceiptHandle] = groupID
		} else {
			// Otherwise, add to pending by group ID
			msg.ReceiptHandle = ""
			msg.Status = common.MessageVisible

			if _, exists := ms.pendingMessages[groupID]; !exists {
				ms.pendingMessages[groupID] = make([]*common.MessageWithVisibility, 0)
			}
			ms.pendingMessages[groupID] = append(ms.pendingMessages[groupID], msg)
		}
	}
}

// RemoveMessageByReceipt removes a message by its receipt handle
func (ms *messageStore) RemoveMessageByReceipt(receiptHandle string) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.inFlightMessages[receiptHandle]; exists {
		delete(ms.inFlightMessages, receiptHandle)
		delete(ms.receiptToGroup, receiptHandle)
		return true
	}
	return false
}

// GetMessageByReceipt retrieves a message by its receipt handle
func (ms *messageStore) GetMessageByReceipt(receiptHandle string) (*common.MessageWithVisibility, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	msg, exists := ms.inFlightMessages[receiptHandle]
	return msg, exists
}

// ResetVisibility makes an in-flight message visible again
func (ms *messageStore) ResetVisibility(receiptHandle string) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	msg, exists := ms.inFlightMessages[receiptHandle]
	if !exists {
		return false
	}

	groupID, exists := ms.receiptToGroup[receiptHandle]
	if !exists {
		// Fallback to the attributes if not found in our index
		groupID = msg.Attributes.MessageGroupID
	}

	// Reset the message
	msg.Status = common.MessageVisible
	msg.InvisibleUntil = time.Time{}
	msg.ReceiptHandle = ""

	// Move from in-flight back to pending
	if _, exists := ms.pendingMessages[groupID]; !exists {
		ms.pendingMessages[groupID] = make([]*common.MessageWithVisibility, 0)
	}
	ms.pendingMessages[groupID] = append(ms.pendingMessages[groupID], msg)

	// Clean up the in-flight entry
	delete(ms.inFlightMessages, receiptHandle)
	delete(ms.receiptToGroup, receiptHandle)

	return true
}

// GetVisibleMessage finds the first visible message in any message group
func (ms *messageStore) GetVisibleMessage() (fifoMessageIndex, *common.MessageWithVisibility, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Check each message group for visible messages
	for groupID, msgs := range ms.pendingMessages {
		for i, msg := range msgs {
			if msg.IsVisible() && !msg.IsDelayed() {
				return fifoMessageIndex{groupID: groupID, pos: i}, msg, true
			}
		}
	}

	return fifoMessageIndex{}, nil, false
}

// MarkMessageInFlight moves a message from pending to in-flight
func (ms *messageStore) MarkMessageInFlight(ix fifoMessageIndex, receiptHandle string, visibilityTimeout time.Duration) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Get the message from pending
	msgs, ok := ms.pendingMessages[ix.groupID]
	if !ok || ix.pos < 0 || ix.pos >= len(msgs) {
		return false
	}

	msg := msgs[ix.pos]

	// Update message state
	msg.Status = common.MessageInFlight
	msg.ReceiptHandle = receiptHandle
	msg.InvisibleUntil = time.Now().Add(visibilityTimeout)
	msg.ReceiveCount++

	// Move from pending to in-flight
	ms.inFlightMessages[receiptHandle] = msg
	ms.receiptToGroup[receiptHandle] = ix.groupID

	// Remove from pending
	ms.pendingMessages[ix.groupID] = slices.Delete(msgs, ix.pos, ix.pos+1)

	// Clean up empty group if needed
	if len(ms.pendingMessages[ix.groupID]) == 0 {
		delete(ms.pendingMessages, ix.groupID)
	}

	return true
}

// GetMessageCount returns counts of visible and in-flight messages
func (ms *messageStore) GetMessageCount() (visible int, inFlight int) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	visible = 0
	for _, msgs := range ms.pendingMessages {
		for _, msg := range msgs {
			if msg.IsVisible() && !msg.IsDelayed() {
				visible++
			}
		}
	}

	inFlight = len(ms.inFlightMessages)
	return
}
