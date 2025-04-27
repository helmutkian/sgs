package fifo

import (
	"fmt"
	"math/big"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// getNextSequenceNumber generates the next sequence number for a message group
func (q *Queue) getNextSequenceNumber(groupID string) string {
	q.SequenceNumberMutex.Lock()
	defer q.SequenceNumberMutex.Unlock()

	// Get current sequence number or initialize to 0
	seq, ok := q.SequenceNumbers[groupID]
	if !ok {
		seq = new(big.Int)
		q.SequenceNumbers[groupID] = seq
	} else {
		// Make a copy of the existing sequence number
		seq = new(big.Int).Set(seq)
	}

	// Increment the sequence number
	seq.Add(seq, big.NewInt(1))

	// Store back the updated sequence number
	q.SequenceNumbers[groupID] = seq

	// Format as string with fixed width (40 chars) to ensure sorting works correctly
	// AWS SQS uses 128-bit integers, which can be up to 39 decimal digits
	// Format with leading zeros to maintain sortability
	return fmt.Sprintf("%040s", seq.String())
}

// isDuplicate checks if a message is a duplicate based on deduplication ID
func (q *Queue) isDuplicate(deduplicationID string) bool {
	if deduplicationID == "" {
		return false
	}

	q.DeduplicationMutex.RLock()
	defer q.DeduplicationMutex.RUnlock()

	_, exists := q.DeduplicationCache[deduplicationID]
	return exists
}

// markMessageProcessed adds a deduplication ID to the cache
func (q *Queue) markMessageProcessed(deduplicationID string) {
	if deduplicationID == "" {
		return
	}

	q.DeduplicationMutex.Lock()
	defer q.DeduplicationMutex.Unlock()

	q.DeduplicationCache[deduplicationID] = time.Now()
}

// getDeduplicationID generates a deduplication ID for a message
func (q *Queue) getDeduplicationID(msg common.Message) string {
	deduplicationID := msg.Attributes.MessageDeduplicationID
	if deduplicationID == "" && q.ContentBasedDeduplication {
		// Generate content-based deduplication ID if not provided and enabled
		deduplicationID = fmt.Sprintf("%x", msg.Body)
		msg.Attributes.MessageDeduplicationID = deduplicationID
	} else if deduplicationID == "" {
		// If no deduplication ID provided and content-based is disabled, generate a unique one
		deduplicationID = fmt.Sprintf("msg-%d-%s", time.Now().UnixNano(), msg.Attributes.MessageGroupID)
		msg.Attributes.MessageDeduplicationID = deduplicationID
	}

	return deduplicationID
}

// SendMessage adds a message to the queue
func (q *Queue) SendMessage(msg common.Message) {
	q.SendMessageWithDelay(msg, 0)
}

// SendMessageWithDelay adds a message with a specified delay
func (q *Queue) SendMessageWithDelay(msg common.Message, delaySeconds int64) {
	// Validate FIFO requirements
	if msg.Attributes.MessageGroupID == "" {
		// AWS requires MessageGroupID for FIFO queues
		return
	}

	// Check for duplicate message
	deduplicationID := q.getDeduplicationID(msg)
	if q.isDuplicate(deduplicationID) {
		// Skip duplicate message
		return
	}

	// Generate sequence number
	if msg.Attributes.SequenceNumber == "" {
		msg.Attributes.SequenceNumber = q.getNextSequenceNumber(msg.Attributes.MessageGroupID)
	}

	// Mark as processed for deduplication
	q.markMessageProcessed(deduplicationID)

	// Send to the underlying queue implementation
	q.BaseQueue.SendMessageWithDelay(msg, delaySeconds)
}

// SendMessageBatch sends multiple messages to the queue at once
func (q *Queue) SendMessageBatch(messages []common.SendMessageBatchItem) []common.BatchResult {
	results := make([]common.BatchResult, len(messages))
	validMessages := make([]common.SendMessageBatchItem, 0, len(messages))

	// Group messages by message group ID to maintain sequence order
	messagesByGroup := make(map[string][]common.SendMessageBatchItem)
	for i, item := range messages {
		groupID := item.Message.Attributes.MessageGroupID
		messagesByGroup[groupID] = append(messagesByGroup[groupID], item)

		// Initialize results
		results[i] = common.BatchResult{
			Success:   true,
			ID:        fmt.Sprintf("msg-%d", i),
			MessageID: item.Message.Attributes.MessageID,
		}
	}

	// Process each group separately to ensure sequence numbers are assigned correctly
	for groupID, groupMessages := range messagesByGroup {
		for _, item := range groupMessages {
			msg := item.Message

			// Find the original index
			var resultIdx int
			for j, origItem := range messages {
				if origItem.Message.Attributes.MessageID == msg.Attributes.MessageID {
					resultIdx = j
					break
				}
			}

			// Validate FIFO requirements
			if msg.Attributes.MessageGroupID == "" {
				results[resultIdx].Success = false
				continue
			}

			// Check for duplicate message
			deduplicationID := q.getDeduplicationID(msg)
			if q.isDuplicate(deduplicationID) {
				// Skip duplicate message but mark as success (SQS behavior)
				continue
			}

			// Generate sequence number
			if msg.Attributes.SequenceNumber == "" {
				msg.Attributes.SequenceNumber = q.getNextSequenceNumber(groupID)
			}

			// Mark as processed for deduplication
			q.markMessageProcessed(deduplicationID)
			item.Message = msg
			validMessages = append(validMessages, item)
		}
	}

	if len(validMessages) > 0 {
		q.BaseQueue.SendMessageBatch(validMessages)
	}

	return results
}

// ForceRemoveDeduplicationID removes a specified deduplication ID from the cache.
// This is primarily used for testing.
// TODO: Provide a better API for this
func (q *Queue) ForceRemoveDeduplicationID(deduplicationID string) {
	q.DeduplicationMutex.Lock()
	defer q.DeduplicationMutex.Unlock()

	delete(q.DeduplicationCache, deduplicationID)
}
