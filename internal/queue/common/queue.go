package common

import (
	"context"
	"iter"
	"time"
)

// MessageProcessor is a function type that processes a message and returns a bool indicating success
type MessageProcessor func(ctx context.Context, msg *MessageWithVisibility) (bool, error)

// BatchResult represents the result of a batch operation for a single entry
type BatchResult struct {
	Success       bool
	ID            string
	ReceiptHandle string
	MessageID     string
}

// Default values for queue configuration
const (
	DefaultVisibilityTimeout     = 30 * time.Second
	DefaultMaxReceiveCount       = 10
	DefaultMaxDelay              = 15 * time.Minute
	DefaultReceiveAttemptTimeout = 5 * time.Minute
	DefaultDeduplicationInterval = 5 * time.Minute
	DefaultMaxConcurrency        = 1
	BatchResultFailureMessage    = "Receipt handle not found or message already deleted"
)

type ChangeMessageVisibilityBatchItem struct {
	ReceiptHandle string
	Timeout       time.Duration
}

type SendMessageBatchItem struct {
	Message      Message
	DelaySeconds int64
}

type MessageStore[I any] interface {
	// Iterator for all messages
	Messages() iter.Seq2[I, *MessageWithVisibility]

	// Basic operations
	Message(index I) (*MessageWithVisibility, bool)
	TotalMessages() int
	RemoveMessage(index I) bool
	AddMessages(msgs ...*MessageWithVisibility)

	// Receipt-handle based operations
	GetMessageByReceipt(receiptHandle string) (*MessageWithVisibility, bool)
	RemoveMessageByReceipt(receiptHandle string) bool

	// Visibility management
	ResetVisibility(receiptHandle string) bool
	GetVisibleMessage() (I, *MessageWithVisibility, bool)
	MarkMessageInFlight(index I, receiptHandle string, visibilityTimeout time.Duration) bool

	// Statistics
	GetMessageCount() (visible int, inFlight int)
}

type ReceiverQueue interface {
	// Basic message operations
	ReceiveMessage(visibilityTimeout time.Duration) (Message, string, bool)
	ReceiveMessageWithAttemptID(receiveRequestAttemptID string, visibilityTimeout time.Duration) (Message, string, bool)
	DeleteMessage(receiptHandle string) bool
	ChangeMessageVisibility(receiptHandle string, timeout time.Duration) bool

	// Batch operations
	ReceiveMessageBatch(maxMessages int, visibilityTimeout time.Duration) ([]Message, []string, bool)
	ReceiveMessageBatchWithAttemptID(receiveRequestAttemptID string, maxMessages int, visibilityTimeout time.Duration) ([]Message, []string, bool)
	DeleteMessageBatch(receiptHandles []string) []BatchResult
	ChangeMessageVisibilityBatch(items []ChangeMessageVisibilityBatchItem) []BatchResult

	// Configuration
	SetConcurrency(maxConcurrency int)
	SetVisibilityTimeout(timeout time.Duration)
	SetMessageProcessor(processor MessageProcessor)

	// Lifecycle
	Start()
	Close()

	// Statistics
	GetQueueStats() map[string]interface{}
}

type DeadLetterQueue interface {
	SendMessage(msg Message)
	ReceiverQueue
	// TODO
	// RedriveToSourceQueue(receiptHandle string) bool
}

// ShouldSendToDeadLetter checks if a message should be sent to the dead letter queue
func ShouldSendToDeadLetter(receiveCount, maxReceiveCount int) bool {
	return maxReceiveCount > 0 && receiveCount >= maxReceiveCount
}

// QueueOperations defines the basic operations that any queue must support
type Queue interface {
	ReceiverQueue
	SendMessage(msg Message)
	SendMessageWithDelay(msg Message, delaySeconds int64)
	SendMessageBatch(messages []SendMessageBatchItem) []BatchResult
}
