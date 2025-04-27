package fifo

import (
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

// getVisibleMessageGroups returns a list of message groups that have visible messages
func (q *Queue) getVisibleMessageGroups() []string {
	groups := make(map[string]bool)
	for _, msg := range q.MessageStore.Messages() {
		if !msg.IsVisible() {
			continue
		}
		if msg.Attributes.MessageGroupID == "" {
			continue
		}
		groups[msg.Attributes.MessageGroupID] = true
	}

	result := make([]string, 0, len(groups))
	for group := range groups {
		result = append(result, group)
	}
	return result
}

// ReceiveMessage receives a single message from the queue
func (q *Queue) ReceiveMessage(visibilityTimeout time.Duration) (common.Message, string, bool) {
	return q.ReceiveMessageWithAttemptID("", visibilityTimeout)
}

// ReceiveMessageWithAttemptID receives a message with attempt ID for deduplication
func (q *Queue) ReceiveMessageWithAttemptID(
	receiveRequestAttemptID string,
	visibilityTimeout time.Duration,
) (common.Message, string, bool) {
	// Check for existing attempt first
	if receiveRequestAttemptID != "" {
		attempt, exists := q.GetReceiveAttempt(receiveRequestAttemptID)
		if exists && len(attempt.ReceiptHandles) > 0 {
			// Find the message for this receipt handle
			q.Mu.Lock()

			msg, found := q.MessageStore.(*messageStore).GetMessageByReceipt(attempt.ReceiptHandles[0])
			if found && msg.IsInFlight() {
				q.Mu.Unlock()
				return msg.Message, msg.ReceiptHandle, true
			}

			q.Mu.Unlock()
		}
	}

	// No existing attempt found, proceed with regular receive
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// Wait until there are messages or shutdown
	if !q.BaseQueue.WaitForVisibleMessages(q.hasVisibleMessages) {
		return common.Message{}, "", false
	}

	// In high throughput mode, we can receive messages from any group that has visible messages
	// In standard mode, we need to ensure strict FIFO ordering within each group
	visibleGroups := q.getVisibleMessageGroups()
	if len(visibleGroups) == 0 {
		return common.Message{}, "", false
	}

	// First, try to find an unlocked message group
	q.MessageGroupMutex.Lock()
	var targetGroup string

	for _, group := range visibleGroups {
		if q.MessageGroupLocks[group] {
			continue
		}
		targetGroup = group
		q.MessageGroupLocks[group] = true
		break
	}
	q.MessageGroupMutex.Unlock()

	// If we couldn't find an unlocked group and we're in high throughput mode,
	// we can still receive from other groups
	if targetGroup == "" && q.HighThroughputMode {
		targetGroup = visibleGroups[0]
	}

	// If we still don't have a target group, no messages are available
	if targetGroup == "" {
		return common.Message{}, "", false
	}

	// Find the first visible message in the target group
	var targetMessage *common.MessageWithVisibility
	var targetIndex fifoMessageIndex

	// Use the message store methods to access messages
	store := q.MessageStore.(*messageStore)
	pendingMsgs, ok := store.pendingMessages[targetGroup]
	if ok {
		for i, msg := range pendingMsgs {
			if msg.IsVisible() && !msg.IsDelayed() {
				targetMessage = msg
				targetIndex = fifoMessageIndex{groupID: targetGroup, pos: i}
				break
			}
		}
	}

	if targetMessage == nil {
		// No visible message found, release the lock and return
		q.MessageGroupMutex.Lock()
		delete(q.MessageGroupLocks, targetGroup)
		q.MessageGroupMutex.Unlock()
		return common.Message{}, "", false
	}

	// Update message status and move to in-flight collection
	receiptHandle := q.GenerateReceiptHandle()
	q.MessageStore.(*messageStore).MarkMessageInFlight(targetIndex, receiptHandle, visibilityTimeout)

	// Record the attempt if an ID was provided
	if receiveRequestAttemptID != "" {
		q.RecordReceiveAttempt(receiveRequestAttemptID, []string{receiptHandle}, common.DefaultReceiveAttemptTimeout)
	}

	// In high throughput mode, release the group lock immediately after message receive
	if q.HighThroughputMode {
		q.MessageGroupMutex.Lock()
		delete(q.MessageGroupLocks, targetGroup)
		q.MessageGroupMutex.Unlock()
	}

	return targetMessage.Message, receiptHandle, true
}

// ReceiveMessageBatch receives multiple messages from the queue at once
func (q *Queue) ReceiveMessageBatch(
	maxMessages int,
	visibilityTimeout time.Duration,
) ([]common.Message, []string, bool) {
	return q.ReceiveMessageBatchWithAttemptID("", maxMessages, visibilityTimeout)
}

// In standard mode, we need to ensure we only get one message per group
func (q *Queue) receiveMessageBatchStdThroughput(
	messages []common.Message,
	receiptHandles []string,
	receiveRequestAttemptID string,
	maxMessages int,
	visibilityTimeout time.Duration,
) ([]common.Message, []string, bool) {
	// Getting a single message will properly respect group locks
	msg, receipt, ok := q.ReceiveMessageWithAttemptID(receiveRequestAttemptID, visibilityTimeout)
	if !ok {
		return nil, nil, false
	}

	messages = append(messages, msg)
	receiptHandles = append(receiptHandles, receipt)

	// For standard mode, query different message groups up to maxMessages
	// First, get all active message groups
	q.MessageGroupMutex.Lock()
	lockedGroups := make(map[string]bool)
	for group, locked := range q.MessageGroupLocks {
		if !locked {
			continue
		}
		lockedGroups[group] = true
	}
	q.MessageGroupMutex.Unlock()

	// Now try to receive from groups that aren't locked
	for i := 1; i < maxMessages; i++ {
		q.Mu.Lock()

		// Look for any group with visible messages that isn't locked
		store := q.MessageStore.(*messageStore)

		// Iterate through all pending message groups (that aren't locked)
		var validGroupID string
		var validIndex int
		var validMsg *common.MessageWithVisibility

		// Safely access the pendingMessages map
		store.mu.RLock()
		for groupID, msgs := range store.pendingMessages {
			// Skip already locked groups
			if lockedGroups[groupID] {
				continue
			}

			// Find a visible message in this group
			for j, m := range msgs {
				if m.IsVisible() && !m.IsDelayed() {
					validMsg = m
					validGroupID = groupID
					validIndex = j
					break
				}
			}

			if validMsg != nil {
				break
			}
		}
		store.mu.RUnlock()

		if validMsg == nil {
			// No more valid messages to receive
			q.Mu.Unlock()
			break
		}

		// Mark the group as locked
		q.MessageGroupMutex.Lock()
		q.MessageGroupLocks[validGroupID] = true
		lockedGroups[validGroupID] = true
		q.MessageGroupMutex.Unlock()

		// Update message status using the message store
		receiptHandle := q.GenerateReceiptHandle()
		targetIndex := fifoMessageIndex{groupID: validGroupID, pos: validIndex}
		store.MarkMessageInFlight(targetIndex, receiptHandle, visibilityTimeout)

		messages = append(messages, validMsg.Message)
		receiptHandles = append(receiptHandles, receiptHandle)

		q.Mu.Unlock()
	}

	if len(messages) == 0 {
		return nil, nil, false
	}

	return messages, receiptHandles, true
}

// ReceiveMessageBatchWithAttemptID receives multiple messages with attempt ID for deduplication
func (q *Queue) ReceiveMessageBatchWithAttemptID(
	receiveRequestAttemptID string,
	maxMessages int,
	visibilityTimeout time.Duration,
) ([]common.Message, []string, bool) {
	// In FIFO queues, batch receive works differently than in standard queues
	// We need to respect message group ordering and locks

	if maxMessages <= 0 {
		maxMessages = 1
	}

	messages := make([]common.Message, 0, maxMessages)
	receiptHandles := make([]string, 0, maxMessages)

	if !q.HighThroughputMode {
		return q.receiveMessageBatchStdThroughput(messages, receiptHandles, receiveRequestAttemptID, maxMessages, visibilityTimeout)
	}

	// In high throughput mode, we can get multiple messages per group
	for range maxMessages {
		msg, receipt, ok := q.ReceiveMessageWithAttemptID(receiveRequestAttemptID, visibilityTimeout)
		if !ok {
			break
		}

		messages = append(messages, msg)
		receiptHandles = append(receiptHandles, receipt)
	}

	if len(messages) == 0 {
		return nil, nil, false
	}

	return messages, receiptHandles, true
}

// hasVisibleMessages checks if there are any visible messages in FIFO groups
func (q *Queue) hasVisibleMessages() bool {
	// Use getVisibleMessageGroups to find any message groups with visible messages
	groups := q.getVisibleMessageGroups()
	return len(groups) > 0
}
