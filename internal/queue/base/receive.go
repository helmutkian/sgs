package base

import (
	"fmt"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// hasVisibleMessages and waitForVisibleMessages are now implemented in visibility.go

// getMessagesReceiptHandles returns a slice of receipt handles from a slice of messages
func getMessagesReceiptHandles(messages []*common.MessageWithVisibility) []string {
	receiptHandles := make([]string, 0, len(messages))
	for _, msg := range messages {
		receiptHandles = append(receiptHandles, msg.ReceiptHandle)
	}
	return receiptHandles
}

// withoutVisibility returns a slice of messages without visibility information
func withoutVisibility(messages []*common.MessageWithVisibility) []common.Message {
	msgs := make([]common.Message, 0, len(messages))
	for _, msg := range messages {
		msgs = append(msgs, msg.Message)
	}
	return msgs
}

func (q *BaseQueue[I]) getAttemptMessages(attemptID string) []*common.MessageWithVisibility {
	attempt, exists := q.GetReceiveAttempt(attemptID)
	if !exists {
		return nil
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	messages := make([]*common.MessageWithVisibility, 0, len(attempt.ReceiptHandles))
	for _, receiptHandle := range attempt.ReceiptHandles {
		// Use the direct method to retrieve message by receipt handle
		if msg, found := q.MessageStore.GetMessageByReceipt(receiptHandle); found && msg.IsInFlight() {
			messages = append(messages, msg)
		}
	}

	return messages
}

// generateReceiptHandle generates a unique receipt handle for a message
func (q *BaseQueue[I]) generateReceiptHandle() string {
	q.ReceiptCounter++
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), q.ReceiptCounter)
}

func (q *BaseQueue[I]) getVisibleMessages(
	hasQueueLock bool,
	maxMessages int,
	visibilityTimeout time.Duration,
) []*common.MessageWithVisibility {
	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > defaultMaxReceiveCount {
		maxMessages = defaultMaxReceiveCount
	}

	if !hasQueueLock {
		q.Mu.Lock()
		defer q.Mu.Unlock()
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
		receiptHandle := q.generateReceiptHandle()

		// Mark the message as in-flight
		if q.MessageStore.MarkMessageInFlight(index, receiptHandle, visibilityTimeout) {
			messages = append(messages, msg)
		}
	}

	return messages
}

func (q *BaseQueue[I]) recordAttempt(hasQueueLock bool, attemptID string, receiptHandles []string) {
	if attemptID == "" {
		return
	}

	if hasQueueLock {
		// Temporarily release the main lock
		q.Mu.Unlock()
		defer q.Mu.Lock()
	}

	q.AttemptsMutex.Lock()
	defer q.AttemptsMutex.Unlock()

	q.ReceiveAttempts[attemptID] = &ReceiveAttempt{
		ID:             attemptID,
		Timestamp:      time.Now(),
		ReceiptHandles: receiptHandles,
		ExpiresAt:      time.Now().Add(defaultReceiveAttemptTimeout),
	}
}

// ReceiveMessage receives a single message from the queue
func (q *BaseQueue[I]) ReceiveMessage(visibilityTimeout time.Duration) (common.Message, string, bool) {
	return q.ReceiveMessageWithAttemptID("", visibilityTimeout)
}

// ReceiveMessageBatch receives multiple messages from the queue at once
func (q *BaseQueue[I]) ReceiveMessageBatch(
	maxMessages int,
	visibilityTimeout time.Duration,
) ([]common.Message, []string, bool) {
	return q.ReceiveMessageBatchWithAttemptID("", maxMessages, visibilityTimeout)
}

// ReceiveMessageWithAttemptID receives a message with an attempt ID for deduplication
func (q *BaseQueue[I]) ReceiveMessageWithAttemptID(
	receiveRequestAttemptID string,
	visibilityTimeout time.Duration,
) (common.Message, string, bool) {
	messages, receiptHandles, ok := q.ReceiveMessageBatchWithAttemptID(receiveRequestAttemptID, 1, visibilityTimeout)
	if !ok {
		return common.Message{}, "", false
	}

	var message common.Message
	if len(messages) > 0 {
		message = messages[0]
	}

	var receiptHandle string
	if len(receiptHandles) > 0 {
		receiptHandle = receiptHandles[0]
	}

	return message, receiptHandle, ok
}

func (q *BaseQueue[I]) ReceiveMessages(
	receiveRequestAttemptID string,
	maxMessages int,
	visibilityTimeout time.Duration,
) []*common.MessageWithVisibility {
	// Check for existing attempt if ID provided
	if receiveRequestAttemptID != "" {
		attemptMessages := q.getAttemptMessages(receiveRequestAttemptID)
		if len(attemptMessages) > 0 {
			return attemptMessages
		}
	}

	// Regular batch receive logic if no existing attempt or no messages found
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// Wait until there are messages or shutdown
	if !q.WaitForVisibleMessages(true) {
		return nil
	}

	messages := q.getVisibleMessages(true, maxMessages, visibilityTimeout)
	receiptHandles := getMessagesReceiptHandles(messages)

	// If we have messages and an attempt ID was provided, record the attempt
	if len(messages) > 0 && receiveRequestAttemptID != "" {
		q.recordAttempt(true, receiveRequestAttemptID, receiptHandles)
	}

	return messages
}

// ReceiveMessageBatchWithAttemptID receives multiple messages with attempt ID for deduplication
func (q *BaseQueue[I]) ReceiveMessageBatchWithAttemptID(
	receiveRequestAttemptID string,
	maxMessages int,
	visibilityTimeout time.Duration,
) ([]common.Message, []string, bool) {
	messages := q.ReceiveMessages(receiveRequestAttemptID, maxMessages, visibilityTimeout)
	return withoutVisibility(messages), getMessagesReceiptHandles(messages), len(messages) > 0
}

// ChangeMessageVisibility changes the visibility timeout of a message
func (q *BaseQueue[I]) ChangeMessageVisibility(receiptHandle string, timeout time.Duration) bool {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// Use the helper method
	_, success := q.changeMessageVisibility(receiptHandle, timeout)
	return success
}
