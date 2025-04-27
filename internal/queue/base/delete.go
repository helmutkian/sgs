package base

import (
	"slices"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// MessageDeletionInfo stores information about a message to be deleted
type MessageDeletionInfo[I any] struct {
	Index         I
	Message       *common.MessageWithVisibility
	ReceiptHandle string
}

// DeleteMessage removes a message from the queue
func (q *BaseQueue[I]) DeleteMessage(receiptHandle string) bool {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// Check if the message exists
	_, found := q.MessageStore.GetMessageByReceipt(receiptHandle)
	if !found {
		return false
	}

	// Use the direct remove by receipt handle method
	return q.MessageStore.RemoveMessageByReceipt(receiptHandle)
}

// DeleteMessageBatch deletes multiple messages at once
func (q *BaseQueue[I]) DeleteMessageBatch(receiptHandles []string) []common.BatchResult {
	results := make([]common.BatchResult, len(receiptHandles))
	for i := range results {
		results[i] = common.BatchResult{
			Success:       false,
			ReceiptHandle: receiptHandles[i],
		}
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	// Process each receipt handle
	for i, receiptHandle := range receiptHandles {
		if receiptHandle == "" {
			continue
		}

		// Get the message first to capture the message ID
		msg, found := q.MessageStore.GetMessageByReceipt(receiptHandle)
		if !found {
			continue
		}

		// Delete the message using its receipt handle
		if q.MessageStore.RemoveMessageByReceipt(receiptHandle) {
			results[i].Success = true
			results[i].MessageID = msg.Attributes.MessageID
		}
	}

	return results
}

// The following methods are kept for backward compatibility with queue implementations
// that haven't been updated to use the new message store methods directly

// CollectMessagesToDelete finds all messages matching receipt handles without modifying anything
func (q *BaseQueue[I]) CollectMessagesToDelete(receiptHandles []string) []MessageDeletionInfo[I] {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	var messagesToDelete []MessageDeletionInfo[I]

	for index, msg := range q.MessageStore.Messages() {
		if msg == nil {
			continue
		}

		if !msg.IsInFlight() {
			continue
		}

		if !slices.Contains(receiptHandles, msg.ReceiptHandle) {
			continue
		}

		messagesToDelete = append(messagesToDelete, MessageDeletionInfo[I]{
			Index:         index,
			Message:       msg,
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	return messagesToDelete
}

// DeleteCollectedMessages removes the collected messages
func (q *BaseQueue[I]) DeleteCollectedMessages(messagesToDelete []MessageDeletionInfo[I]) map[string]*common.MessageWithVisibility {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	deletedMessagesByReceiptHandle := make(map[string]*common.MessageWithVisibility)

	for _, info := range messagesToDelete {
		if q.MessageStore.RemoveMessage(info.Index) {
			deletedMessagesByReceiptHandle[info.ReceiptHandle] = info.Message
		}
	}

	return deletedMessagesByReceiptHandle
}

// DeleteMessages is a convenience method that combines direct message deletion
func (q *BaseQueue[I]) DeleteMessages(receiptHandles []string) map[string]*common.MessageWithVisibility {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	deletedMessages := make(map[string]*common.MessageWithVisibility)

	for _, receiptHandle := range receiptHandles {
		if receiptHandle == "" {
			continue
		}

		// Get the message before deleting it
		msg, found := q.MessageStore.GetMessageByReceipt(receiptHandle)
		if !found {
			continue
		}

		// Remove the message by receipt handle
		if q.MessageStore.RemoveMessageByReceipt(receiptHandle) {
			deletedMessages[receiptHandle] = msg
		}
	}

	return deletedMessages
}
