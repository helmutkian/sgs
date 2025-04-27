package base

import (
	"fmt"
	"sync"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

const (
	defaultMaxConcurrency        = common.DefaultMaxConcurrency
	defaultVisibilityTimeout     = common.DefaultVisibilityTimeout
	defaultMaxReceiveCount       = common.DefaultMaxReceiveCount
	defaultMaxDelay              = common.DefaultMaxDelay
	defaultReceiveAttemptTimeout = common.DefaultReceiveAttemptTimeout
)

type (
	MessageProcessor    = common.MessageProcessor
	MessageStore[I any] = common.MessageStore[I]
)

// ReceiveAttempt tracks information about a receive message request
type ReceiveAttempt struct {
	ID             string    // The receive request attempt ID
	Timestamp      time.Time // When this attempt was made
	ReceiptHandles []string  // Receipt handles returned in this attempt
	ExpiresAt      time.Time // When this attempt record expires
}

// BaseQueue contains common fields and methods for both standard and FIFO queues
type BaseQueue[I any] struct {
	MessageStore MessageStore[I]
	Mu           sync.Mutex
	Cond         *sync.Cond
	Shutdown     chan struct{}

	// Concurrency management
	MaxConcurrency    int
	ActiveWorkers     int
	ProcessorPool     chan bool
	WorkerWG          sync.WaitGroup
	ProcessorFunc     MessageProcessor
	DefaultVisibility time.Duration
	ReceiptCounter    int64

	// Dead letter queue management
	MaxReceiveCount int

	// Request attempt tracking
	ReceiveAttempts map[string]*ReceiveAttempt
	AttemptsMutex   sync.RWMutex
}

// NewBaseQueue initializes a base queue with default settings
func NewBaseQueue[I any](
	msgStore MessageStore[I],
) *BaseQueue[I] {
	q := &BaseQueue[I]{
		MessageStore:      msgStore,
		Shutdown:          make(chan struct{}),
		MaxConcurrency:    defaultMaxConcurrency,
		ProcessorPool:     make(chan bool, defaultMaxConcurrency),
		DefaultVisibility: defaultVisibilityTimeout,
		MaxReceiveCount:   defaultMaxReceiveCount,
		ReceiveAttempts:   make(map[string]*ReceiveAttempt),
	}

	q.Cond = sync.NewCond(&q.Mu)

	// Initialize the semaphore
	q.ProcessorPool <- true

	return q
}

// SetConcurrency configures the queue for concurrent message processing
func (q *BaseQueue[I]) SetConcurrency(maxConcurrency int) {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	// If reducing concurrency, don't touch the existing workers
	// They'll naturally exit when they're done
	if maxConcurrency <= q.MaxConcurrency {
		q.MaxConcurrency = maxConcurrency
		return
	}

	// Expand the processor pool
	oldCap := cap(q.ProcessorPool)
	newPool := make(chan bool, maxConcurrency)

	// Move existing tokens to the new pool
	for i := 0; i < oldCap; i++ {
		select {
		case token := <-q.ProcessorPool:
			newPool <- token
		default:
			// This worker is active
		}
	}

	// Add new tokens
	for i := oldCap; i < maxConcurrency; i++ {
		newPool <- true
	}

	q.ProcessorPool = newPool

	q.MaxConcurrency = maxConcurrency
}

// SetMessageProcessor sets the function that will be called to process messages
func (q *BaseQueue[I]) SetMessageProcessor(processor MessageProcessor) {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	q.ProcessorFunc = processor
}

// SetVisibilityTimeout sets the default visibility timeout for messages
func (q *BaseQueue[I]) SetVisibilityTimeout(timeout time.Duration) {
	if timeout < 1*time.Second {
		timeout = 1 * time.Second // Minimum 1 second
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()
	q.DefaultVisibility = timeout
}

// GenerateReceiptHandle generates a unique receipt handle for a message
func (q *BaseQueue[I]) GenerateReceiptHandle() string {
	q.ReceiptCounter++
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), q.ReceiptCounter)
}

// CleanupReceiveAttempts removes expired receive attempt records
func (q *BaseQueue[I]) CleanupReceiveAttempts() {
	q.AttemptsMutex.Lock()
	defer q.AttemptsMutex.Unlock()

	now := time.Now()
	for id, attempt := range q.ReceiveAttempts {
		if now.After(attempt.ExpiresAt) {
			delete(q.ReceiveAttempts, id)
		}
	}
}

// Close stops the queue and waits for all workers to exit
func (q *BaseQueue[I]) Close() {
	close(q.Shutdown)
	q.WorkerWG.Wait()
}

// GetReceiveAttempt retrieves a receive attempt by ID
func (q *BaseQueue[I]) GetReceiveAttempt(attemptID string) (*ReceiveAttempt, bool) {
	q.AttemptsMutex.RLock()
	defer q.AttemptsMutex.RUnlock()

	attempt, exists := q.ReceiveAttempts[attemptID]
	return attempt, exists
}

// RecordReceiveAttempt records a new receive attempt
func (q *BaseQueue[I]) RecordReceiveAttempt(attemptID string, receiptHandles []string, expiresIn time.Duration) {
	q.AttemptsMutex.Lock()
	defer q.AttemptsMutex.Unlock()

	q.ReceiveAttempts[attemptID] = &ReceiveAttempt{
		ID:             attemptID,
		Timestamp:      time.Now(),
		ReceiptHandles: receiptHandles,
		ExpiresAt:      time.Now().Add(expiresIn),
	}
}

// GetQueueStats returns statistics about this queue
func (q *BaseQueue[I]) GetQueueStats() map[string]any {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	visibleCount := 0
	inFlightCount := 0

	for _, msg := range q.MessageStore.Messages() {
		if msg.IsVisible() {
			visibleCount++
		} else if msg.IsInFlight() {
			inFlightCount++
		}
	}

	return map[string]any{
		"visible_messages":   visibleCount,
		"in_flight_messages": inFlightCount,
		"total_messages":     q.MessageStore.TotalMessages(),
		"max_concurrency":    q.MaxConcurrency,
		"active_workers":     q.ActiveWorkers,
	}
}
