package base

import (
	"fmt"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// makeMessageVisible makes a delayed message visible when its delay expires
func (q *BaseQueue[I]) makeMessageVisible() {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	now := time.Now()
	for _, msg := range q.MessageStore.Messages() {
		if msg.DelayedUntil.IsZero() {
			continue
		}
		if now.Equal(msg.DelayedUntil) || now.Before(msg.DelayedUntil) {
			continue
		}

		msg.Status = common.MessageVisible
		msg.DelayedUntil = time.Time{} // Clear the delay

		// Signal that a message is now available
		q.Cond.Signal()
	}
}

func (q *BaseQueue[I]) SendMessages(messages []common.SendMessageBatchItem) {
	q.Mu.Lock()
	defer q.Mu.Unlock()
	now := time.Now()
	hasVisible := false

	sentMessages := make([]*common.MessageWithVisibility, 0, len(messages))
	for _, msg := range messages {
		sentMessage := &common.MessageWithVisibility{
			Message:      msg.Message,
			Status:       common.MessageVisible,
			ReceiveCount: 0,
		}

		// Use ID if provided in MessageAttributes, otherwise generate one
		sentMessage.Attributes.MessageID = msg.Message.Attributes.MessageID
		if sentMessage.Attributes.MessageID == "" {
			sentMessage.Attributes.MessageID = fmt.Sprintf("msg-%d", time.Now().UnixNano())
		}

		// Compute delay if specified
		delaySeconds := msg.DelaySeconds
		if delaySeconds == 0 {
			hasVisible = true
			sentMessages = append(sentMessages, sentMessage)
			continue
		}
		// Limit to maximum delay
		if delaySeconds > int64(defaultMaxDelay/time.Second) {
			delaySeconds = int64(defaultMaxDelay / time.Second)
		}

		delayDuration := time.Duration(delaySeconds) * time.Second
		delayedUntil := now.Add(delayDuration)
		delayTimer := time.AfterFunc(delayDuration, func() {
			q.makeMessageVisible()
		})

		sentMessage.DelayedUntil = delayedUntil
		sentMessage.DelayTimer = delayTimer
		sentMessage.Status = common.MessageInFlight

		// Add the message to the collection
		sentMessages = append(sentMessages, sentMessage)
	}

	q.MessageStore.AddMessages(sentMessages...)

	// Signal that visible messages are now available
	if hasVisible {
		q.Cond.Signal()
	}
}

// SendMessageBatch sends multiple messages to the queue at once
func (q *BaseQueue[I]) SendMessageBatch(messages []common.SendMessageBatchItem) []common.BatchResult {
	q.SendMessages(messages)
	results := make([]common.BatchResult, len(messages))
	for i, msg := range messages {
		results[i] = common.BatchResult{
			Success:   true,
			MessageID: msg.Message.Attributes.MessageID,
		}
	}

	return results
}

// SendMessageWithDelay adds a message with a specified delay
func (q *BaseQueue[I]) SendMessageWithDelay(msg common.Message, delaySeconds int64) {
	q.SendMessages([]common.SendMessageBatchItem{{
		Message:      msg,
		DelaySeconds: delaySeconds,
	}})
}

func (q *BaseQueue[I]) SendMessage(msg common.Message) {
	q.SendMessageWithDelay(msg, 0)
}
