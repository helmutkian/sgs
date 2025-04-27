package fifo

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/helmutkian/sgs/internal/queue/base"
	"github.com/helmutkian/sgs/internal/queue/common"
)

const (
	PollingInterval                = 100 * time.Millisecond
	DefaultDeduplicationTimeWindow = common.DefaultDeduplicationInterval
)

// FIFOQueue represents an AWS SQS FIFO queue with support for high throughput
type Queue struct {
	*base.BaseQueue[fifoMessageIndex]

	// Deduplication tracking
	DeduplicationWindow time.Duration
	DeduplicationCache  map[string]time.Time
	DeduplicationMutex  sync.RWMutex

	// MessageGroupID tracking for FIFO behavior
	MessageGroupLocks map[string]bool
	MessageGroupMutex sync.Mutex

	// High throughput mode
	HighThroughputMode bool

	// Sequence number tracking (per message group)
	SequenceNumbers     map[string]*big.Int
	SequenceNumberMutex sync.Mutex

	// Content-based deduplication
	ContentBasedDeduplication bool

	// Dead letter queue
	deadLetterQueue *deadLetterQueue
}

// deadLetterQueue wraps the FIFO queue for dead letter functionality
type deadLetterQueue struct {
	*Queue
}

var _ common.Queue = &Queue{}
var _ common.DeadLetterQueue = &deadLetterQueue{}

// NewQueue creates a new FIFO queue
func NewQueue(highThroughputMode bool, isDLQEnabled bool) *Queue {
	q := &Queue{
		BaseQueue:                 base.NewBaseQueue[fifoMessageIndex](newMessageStore()),
		DeduplicationCache:        make(map[string]time.Time),
		DeduplicationWindow:       DefaultDeduplicationTimeWindow,
		MessageGroupLocks:         make(map[string]bool),
		HighThroughputMode:        highThroughputMode,
		SequenceNumbers:           make(map[string]*big.Int),
		ContentBasedDeduplication: false,
	}

	if isDLQEnabled {
		q.deadLetterQueue = &deadLetterQueue{
			Queue: &Queue{
				BaseQueue:                 base.NewBaseQueue[fifoMessageIndex](newMessageStore()),
				DeduplicationCache:        make(map[string]time.Time),
				DeduplicationWindow:       DefaultDeduplicationTimeWindow,
				MessageGroupLocks:         make(map[string]bool),
				HighThroughputMode:        highThroughputMode,
				SequenceNumbers:           make(map[string]*big.Int),
				ContentBasedDeduplication: false,
			},
		}
		q.ProcessorFunc = q.newDLQProcessor(q.ProcessorFunc)
	}

	// Start background goroutines
	go q.fifoVisibilityTimeoutChecker()
	go q.receiveAttemptsCleanup()
	go q.deduplicationCacheCleanup()

	return q
}

// fifoVisibilityTimeoutChecker runs periodically to check for messages whose visibility timeout has expired
// This is a FIFO-specific implementation that handles message group locks
func (q *Queue) fifoVisibilityTimeoutChecker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.checkFIFOVisibilityTimeouts()
		case <-q.Shutdown:
			return
		}
	}
}

// checkFIFOVisibilityTimeouts checks for and resets messages whose visibility timeout has expired
// This is a FIFO-specific implementation that handles message group locks
func (q *Queue) checkFIFOVisibilityTimeouts() {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	now := time.Now()

	// In FIFO queues, we need to track message group IDs to maintain strict ordering
	// When a message becomes visible again, we need to ensure correct ordering
	groupsWithReturnedMessages := make(map[string]bool)

	for _, msg := range q.MessageStore.Messages() {
		if !msg.IsInFlight() {
			continue
		}

		if !now.After(msg.InvisibleUntil) || !msg.DelayedUntil.IsZero() {
			continue
		}

		if msg.ReceiptHandle == "" {
			continue
		}

		ok := q.MessageStore.(*messageStore).ResetVisibility(msg.ReceiptHandle)
		if !ok {
			continue
		}

		if msg.Attributes.MessageGroupID != "" {
			groupsWithReturnedMessages[msg.Attributes.MessageGroupID] = true
		}
	}

	// Release message group locks for groups with returned messages
	if !q.HighThroughputMode && len(groupsWithReturnedMessages) > 0 {
		q.MessageGroupMutex.Lock()
		for groupID := range groupsWithReturnedMessages {
			delete(q.MessageGroupLocks, groupID)
		}
		q.MessageGroupMutex.Unlock()
	}

	// Signal that messages are available
	if len(groupsWithReturnedMessages) > 0 {
		q.Cond.Signal()
	}
}

// receiveAttemptsCleanup periodically cleans up expired receive attempt records
func (q *Queue) receiveAttemptsCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.BaseQueue.CleanupReceiveAttempts()
		case <-q.Shutdown:
			return
		}
	}
}

// deduplicationCacheCleanup periodically cleans up expired deduplication entries
func (q *Queue) deduplicationCacheCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.cleanupDeduplicationCache()
		case <-q.Shutdown:
			return
		}
	}
}

// cleanupDeduplicationCache removes expired deduplication IDs
func (q *Queue) cleanupDeduplicationCache() {
	q.DeduplicationMutex.Lock()
	defer q.DeduplicationMutex.Unlock()

	now := time.Now()
	for id, timestamp := range q.DeduplicationCache {
		if now.Sub(timestamp) > q.DeduplicationWindow {
			delete(q.DeduplicationCache, id)
		}
	}
}

// newDLQProcessor wraps a message processor with DLQ functionality
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
		var targetMessage *common.MessageWithVisibility
		var targetIndex fifoMessageIndex
		var targetGroupID string
		found := false

		// Iterate through all messages to find the one with matching receipt handle
		for ix, qMsg := range q.MessageStore.Messages() {
			if qMsg == nil || msg.ReceiptHandle == "" || qMsg.ReceiptHandle != msg.ReceiptHandle || !qMsg.IsInFlight() {
				continue
			}

			targetMessage = qMsg
			targetIndex = ix
			targetGroupID = qMsg.Attributes.MessageGroupID
			found = true
			break
		}

		if !found {
			return shouldDelete, err
		}

		// Copy the message to preserve its content
		dlqMsg := targetMessage.Message
		dlqMsg.Attributes.OriginalReceiveCount = targetMessage.ReceiveCount

		// Remove the message from the queue
		q.MessageStore.RemoveMessage(targetIndex)

		// If we're in standard mode, release the message group lock
		if !q.HighThroughputMode {
			q.MessageGroupMutex.Lock()
			delete(q.MessageGroupLocks, targetGroupID)
			q.MessageGroupMutex.Unlock()
		}

		// Send to DLQ
		q.deadLetterQueue.SendMessage(dlqMsg)

		return true, err
	}
}

// Start ensures workers are running
func (q *Queue) Start() {
	// Start worker goroutines
	q.ensureWorkersRunning()
}

// Close stops the queue and waits for all workers to exit
func (q *Queue) Close() {
	q.BaseQueue.Close()
	if q.deadLetterQueue != nil {
		q.deadLetterQueue.BaseQueue.Close()
	}
}

// SetMessageProcessor sets the function that will be called to process messages
func (q *Queue) SetMessageProcessor(processor common.MessageProcessor) {
	q.BaseQueue.SetMessageProcessor(processor)
}

// SetVisibilityTimeout sets the default visibility timeout for messages
func (q *Queue) SetVisibilityTimeout(timeout time.Duration) {
	q.BaseQueue.SetVisibilityTimeout(timeout)
}

// SetConcurrency configures the queue for concurrent message processing
func (q *Queue) SetConcurrency(maxConcurrency int) {
	q.BaseQueue.SetConcurrency(maxConcurrency)

	// Start additional worker goroutines if needed
	q.ensureWorkersRunning()
}

// ensureWorkersRunning makes sure we have worker goroutines started
func (q *Queue) ensureWorkersRunning() {
	// Start worker goroutines up to maxConcurrency
	for i := q.ActiveWorkers; i < q.MaxConcurrency; i++ {
		q.WorkerWG.Add(1)
		q.ActiveWorkers++
		go q.worker()
	}
}

// worker is a goroutine that continuously processes available messages
func (q *Queue) worker() {
	defer q.WorkerWG.Done()

	for {
		// First, get a token from the processor pool
		select {
		case <-q.ProcessorPool:
			// Got a token, continue processing
		case <-q.Shutdown:
			return
		}

		// Try to receive a message
		msg, receipt, ok := q.ReceiveMessage(q.DefaultVisibility)
		if !ok {
			// If we couldn't get a message, return the token and wait
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

		// Process the message
		ctx, cancel := context.WithTimeout(context.Background(), q.DefaultVisibility)
		shouldDelete, err := q.ProcessorFunc(ctx, &common.MessageWithVisibility{
			Message:       msg,
			ReceiptHandle: receipt,
		})
		cancel()

		if shouldDelete && err == nil {
			// Delete the message on success
			q.DeleteMessage(receipt)
		} else {
			// Log the failure
			if err != nil {
				fmt.Printf("Failed to process message: %v\n", err)
			} else {
				fmt.Printf("Processing returned false for message\n")
			}
		}

		// Return the token to the pool
		q.ProcessorPool <- true
	}
}

// SetDeduplicationWindow sets the window of time during which duplicates are tracked
func (q *Queue) SetDeduplicationWindow(window time.Duration) {
	q.DeduplicationWindow = window
}

// EnableHighThroughputMode enables high throughput mode for the queue
func (q *Queue) EnableHighThroughputMode() {
	q.HighThroughputMode = true
}

// DisableHighThroughputMode disables high throughput mode for the queue
func (q *Queue) DisableHighThroughputMode() {
	q.HighThroughputMode = false
}

// SetContentBasedDeduplication enables or disables content-based deduplication
func (q *Queue) SetContentBasedDeduplication(enabled bool) {
	q.ContentBasedDeduplication = enabled
}

// GetQueueStats returns statistics about this queue
func (q *Queue) GetQueueStats() map[string]interface{} {
	baseStats := q.BaseQueue.GetQueueStats()

	q.DeduplicationMutex.RLock()
	deduplicationCacheSize := len(q.DeduplicationCache)
	q.DeduplicationMutex.RUnlock()

	q.MessageGroupMutex.Lock()
	lockedGroupCount := len(q.MessageGroupLocks)
	q.MessageGroupMutex.Unlock()

	q.SequenceNumberMutex.Lock()
	sequenceNumberCount := len(q.SequenceNumbers)
	q.SequenceNumberMutex.Unlock()

	baseStats["deduplication_cache_size"] = deduplicationCacheSize
	baseStats["locked_message_groups"] = lockedGroupCount
	baseStats["high_throughput_mode"] = q.HighThroughputMode
	baseStats["sequence_number_groups"] = sequenceNumberCount
	baseStats["content_based_deduplication"] = q.ContentBasedDeduplication

	return baseStats
}

// SendMessage adds a message to the dead letter queue
func (dlq *deadLetterQueue) SendMessage(msg common.Message) {
	// We need to bypass the deduplication for DLQ messages
	// Create unique deduplication ID for DLQ to avoid conflicts
	dlqDedupeID := fmt.Sprintf("dlq-%d-%s", time.Now().UnixNano(), msg.Attributes.MessageDeduplicationID)
	msg.Attributes.MessageDeduplicationID = dlqDedupeID

	// Send directly through the queue implementation
	dlq.Queue.SendMessage(msg)
}

// ForceCheckVisibilityTimeouts manually triggers a check for expired visibility timeouts.
// This is primarily used for testing.
func (q *Queue) ForceCheckVisibilityTimeouts() {
	q.checkFIFOVisibilityTimeouts()
}
