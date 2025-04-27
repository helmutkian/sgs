package standard

import (
	"context"
	"fmt"
	"time"

	"github.com/helmutkian/sgs/internal/queue/base"
	"github.com/helmutkian/sgs/internal/queue/common"
)

// Default values for standard queue configuration
const (
	PollingInterval = 100 * time.Millisecond
)

type queue struct {
	*base.BaseQueue[int]
}

var _ common.ReceiverQueue = &queue{}

type deadLetterQueue struct {
	*queue
}

var _ common.DeadLetterQueue = &deadLetterQueue{}

// Queue represents a standard SQS queue (non-FIFO)
type Queue struct {
	*queue
	deadLetterQueue *deadLetterQueue
}

var _ common.Queue = &Queue{}

func defaultProcessorFunc(ctx context.Context, msg *common.MessageWithVisibility) (bool, error) {
	fmt.Printf("Processing message: %s\n", string(msg.Body))
	time.Sleep(500 * time.Millisecond)
	return true, nil
}

func newQueue(processorFunc common.MessageProcessor) *queue {
	q := &queue{
		BaseQueue: base.NewBaseQueue(newMessageStore()),
	}

	q.ProcessorFunc = processorFunc

	// Start background goroutines
	q.StartVisibilityChecker()
	go q.receiveAttemptsCleanup()

	return q
}

// NewQueue creates a new standard queue
func NewQueue(isDLQEnabled bool) *Queue {
	q := &Queue{
		queue: newQueue(defaultProcessorFunc),
	}

	if isDLQEnabled {
		q.deadLetterQueue = &deadLetterQueue{
			queue: newQueue(defaultProcessorFunc),
		}
		q.ProcessorFunc = q.newDLQProcessor(q.ProcessorFunc)
	}

	return q
}

func (q *Queue) newDLQProcessor(msgProcessor common.MessageProcessor) common.MessageProcessor {
	if msgProcessor == nil {
		msgProcessor = func(ctx context.Context, msg *common.MessageWithVisibility) (bool, error) {
			return true, nil
		}
	}

	return func(ctx context.Context, msg *common.MessageWithVisibility) (bool, error) {
		// Skip nil messages
		if msg == nil {
			return false, nil
		}

		// Run the wrapped processor
		shouldDelete, err := msgProcessor(ctx, msg)

		if q.deadLetterQueue == nil || msg.ReceiveCount < q.MaxReceiveCount {
			return shouldDelete, err
		}

		// If we've reached maxReceiveCount, move to DLQ
		q.Mu.Lock()
		defer q.Mu.Unlock()

		// Find the message in the queue
		if msg.ReceiptHandle == "" {
			return shouldDelete, err
		}

		// Get the message using its receipt handle
		targetMessage, found := q.MessageStore.(*messageStore).GetMessageByReceipt(msg.ReceiptHandle)

		if !found || !targetMessage.IsInFlight() {
			return shouldDelete, err
		}

		// Copy the message to preserve its content
		dlqMsg := targetMessage.Message
		dlqMsg.Attributes.OriginalReceiveCount = targetMessage.ReceiveCount

		// Remove the message from the queue using receipt handle
		q.MessageStore.(*messageStore).RemoveMessageByReceipt(msg.ReceiptHandle)

		// Send to DLQ
		q.deadLetterQueue.SendMessage(dlqMsg)

		return true, err
	}
}

// receiveAttemptsCleanup periodically cleans up expired receive attempt records
func (q *queue) receiveAttemptsCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.BaseQueue.CleanupReceiveAttempts() // Use base implementation
		case <-q.Shutdown:
			return
		}
	}
}

// SetConcurrency configures the queue for concurrent message processing
func (q *queue) SetConcurrency(maxConcurrency int) {
	q.BaseQueue.SetConcurrency(maxConcurrency)

	// Start additional worker goroutines if needed
	q.ensureWorkersRunning()
}

// ensureWorkersRunning makes sure we have worker goroutines started
func (q *queue) ensureWorkersRunning() {
	// Start worker goroutines up to maxConcurrency
	for i := q.ActiveWorkers; i < q.MaxConcurrency; i++ {
		q.WorkerWG.Add(1)
		q.ActiveWorkers++
		go q.worker()
	}
}

// worker is a goroutine that continuously processes available messages
func (q *queue) worker() {
	defer q.WorkerWG.Done()

	for {
		// First, get a token from the processor pool
		select {
		case <-q.ProcessorPool:
			// Got a token, continue processing
		case <-q.Shutdown:
			return
		}

		// Find a message to process
		msgs := q.ReceiveMessages("", 1, q.DefaultVisibility)
		if len(msgs) == 0 {
			// If we couldn't get a message, return the token and exit if shutting down
			q.ProcessorPool <- true

			// Check if we're shutting down
			select {
			case <-q.Shutdown:
				q.Mu.Lock()
				q.ActiveWorkers--
				q.Mu.Unlock()
				return
			default:
				// No message available now, wait a bit and try again
				time.Sleep(PollingInterval)
				continue
			}
		}

		msg := msgs[0]
		receiptHandle := msg.ReceiptHandle

		// Process the message
		ctx, cancel := context.WithTimeout(context.Background(), q.DefaultVisibility)
		shouldDelete, err := q.ProcessorFunc(ctx, msg)
		cancel()

		if shouldDelete && err == nil {
			// Delete the message on success
			q.DeleteMessage(receiptHandle)
			return
		}
		// Log the failure
		if err != nil {
			fmt.Printf("Failed to process message: %v\n", err)
		} else {
			fmt.Printf("Processing returned false for message\n")
		}

		// Return the token to the pool
		q.ProcessorPool <- true
	}
}

// SetMessageProcessor sets the function that will be called to process messages
func (q *queue) SetMessageProcessor(processor common.MessageProcessor) {
	q.BaseQueue.SetMessageProcessor(processor)
}

// SetVisibilityTimeout sets the default visibility timeout for messages
func (q *queue) SetVisibilityTimeout(timeout time.Duration) {
	q.BaseQueue.SetVisibilityTimeout(timeout)
}

// Start starts the queue's message processing goroutines
func (q *queue) Start() {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	q.ensureWorkersRunning()
}

func (q *Queue) Start() {
	q.queue.Start()
	if q.deadLetterQueue != nil {
		q.deadLetterQueue.Start()
	}
}

func (q *Queue) Close() {
	q.queue.Close()
	if q.deadLetterQueue != nil {
		q.deadLetterQueue.Close()
	}
}
