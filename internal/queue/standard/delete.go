package standard

import (
	"github.com/helmutkian/sgs/internal/queue/common"
)

// DeleteMessage removes a message from the queue based on its receipt handle
func (q *queue) DeleteMessage(receiptHandle string) bool {
	if receiptHandle == "" {
		return false
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	// Remove message from in-flight messages
	return q.MessageStore.(*messageStore).RemoveMessageByReceipt(receiptHandle)
}

// DeleteMessageBatch removes multiple messages from the queue based on their receipt handles
func (q *queue) DeleteMessageBatch(receiptHandles []string) []common.BatchResult {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	results := make([]common.BatchResult, len(receiptHandles))

	for i, receiptHandle := range receiptHandles {
		// Initialize the result
		results[i] = common.BatchResult{
			Success:       false,
			ReceiptHandle: receiptHandle,
		}

		if receiptHandle == "" {
			continue
		}

		// Get the message to retrieve its ID
		msg, found := q.MessageStore.(*messageStore).GetMessageByReceipt(receiptHandle)
		if !found {
			continue
		}

		// Remove message
		success := q.MessageStore.(*messageStore).RemoveMessageByReceipt(receiptHandle)
		if success {
			results[i].Success = true
			results[i].MessageID = msg.Attributes.MessageID
		}
	}

	return results
}
