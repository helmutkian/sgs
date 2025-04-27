package standard

import (
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// getVisibleMessages retrieves visible messages up to the specified maximum count
func (q *queue) getVisibleMessages(
	maxMessages int,
	visibilityTimeout time.Duration,
) []*common.MessageWithVisibility {
	if maxMessages <= 0 {
		maxMessages = 1
	}

	messages := make([]*common.MessageWithVisibility, 0, maxMessages)

	// Get messages using the message store's optimized methods
	for i := 0; i < maxMessages; i++ {
		// Get a visible message
		index, msg, found := q.MessageStore.GetVisibleMessage()
		if !found {
			break
		}

		// Generate a receipt handle
		receiptHandle := q.GenerateReceiptHandle()

		// Mark the message as in-flight
		if q.MessageStore.MarkMessageInFlight(index, receiptHandle, visibilityTimeout) {
			messages = append(messages, msg)
		}
	}

	return messages
}

// ReceiveMessage receives a single message from the queue
func (q *queue) ReceiveMessage(visibilityTimeout time.Duration) (common.Message, string, bool) {
	return q.ReceiveMessageWithAttemptID("", visibilityTimeout)
}

// ReceiveMessageWithAttemptID receives a message with an attempt ID for deduplication
func (q *queue) ReceiveMessageWithAttemptID(
	receiveRequestAttemptID string,
	visibilityTimeout time.Duration,
) (common.Message, string, bool) {
	msgs, receiptHandles, ok := q.ReceiveMessageBatchWithAttemptID(receiveRequestAttemptID, 1, visibilityTimeout)
	if !ok {
		return common.Message{}, "", false
	}

	var message common.Message
	if len(msgs) > 0 {
		message = msgs[0]
	}

	var receiptHandle string
	if len(receiptHandles) > 0 {
		receiptHandle = receiptHandles[0]
	}

	return message, receiptHandle, ok
}

// ReceiveMessageBatch receives multiple messages from the queue at once
func (q *queue) ReceiveMessageBatch(
	maxMessages int,
	visibilityTimeout time.Duration,
) ([]common.Message, []string, bool) {
	return q.ReceiveMessageBatchWithAttemptID("", maxMessages, visibilityTimeout)
}

// ReceiveMessageBatchWithAttemptID receives multiple messages with attempt ID for deduplication
func (q *queue) ReceiveMessageBatchWithAttemptID(
	receiveRequestAttemptID string,
	maxMessages int,
	visibilityTimeout time.Duration,
) ([]common.Message, []string, bool) {
	messages := q.ReceiveMessages(receiveRequestAttemptID, maxMessages, visibilityTimeout)
	if len(messages) == 0 {
		return nil, nil, false
	}

	receiptHandles := make([]string, len(messages))
	for i, msg := range messages {
		receiptHandles[i] = msg.ReceiptHandle
	}

	messagesWithoutVisibility := make([]common.Message, len(messages))
	for i, msg := range messages {
		messagesWithoutVisibility[i] = msg.Message
	}

	return messagesWithoutVisibility, receiptHandles, true
}

func (q *queue) checkReceiveAttempt(hasQueueLock bool, receiveRequestAttemptID string) ([]*common.MessageWithVisibility, bool) {
	if receiveRequestAttemptID == "" {
		return nil, false
	}

	attempt, exists := q.GetReceiveAttempt(receiveRequestAttemptID)
	if !exists || len(attempt.ReceiptHandles) == 0 {
		return nil, false
	}

	// Find the messages for these receipt handles
	messages := make([]*common.MessageWithVisibility, 0, len(attempt.ReceiptHandles))
	receiptHandles := make([]string, 0, len(attempt.ReceiptHandles))

	if hasQueueLock {
		q.Mu.Lock()
		defer q.Mu.Unlock()
	}

	for _, handle := range attempt.ReceiptHandles {
		msg, found := q.MessageStore.GetMessageByReceipt(handle)
		if found && msg.IsInFlight() {
			messages = append(messages, msg)
			receiptHandles = append(receiptHandles, handle)
		}
	}

	return messages, len(messages) > 0
}

// ReceiveMessages receives multiple messages with visibility information
func (q *queue) ReceiveMessages(
	receiveRequestAttemptID string,
	maxMessages int,
	visibilityTimeout time.Duration,
) []*common.MessageWithVisibility {
	// Check for existing attempt if ID provided
	messages, ok := q.checkReceiveAttempt(false, receiveRequestAttemptID)
	if ok {
		return messages
	}

	// Regular batch receive logic if no existing attempt or no messages found
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// Wait for visible messages
	if !q.BaseQueue.WaitForVisibleMessages(q.hasVisibleMessages) {
		return nil
	}

	// Get visible messages
	messages = q.getVisibleMessages(maxMessages, visibilityTimeout)

	if len(messages) == 0 || receiveRequestAttemptID == "" {
		return messages
	}

	// If we have messages and an attempt ID was provided, record the attempt
	receiptHandles := make([]string, len(messages))
	for i, msg := range messages {
		receiptHandles[i] = msg.ReceiptHandle
	}
	q.RecordReceiveAttempt(receiveRequestAttemptID, receiptHandles, common.DefaultReceiveAttemptTimeout)

	return messages
}

// hasVisibleMessages checks if there are any visible messages in the standard queue
func (q *queue) hasVisibleMessages() bool {
	for _, msg := range q.MessageStore.Messages() {
		if msg.IsVisible() && !msg.IsDelayed() {
			return true
		}
	}
	return false
}
