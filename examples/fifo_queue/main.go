package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
	"github.com/helmutkian/sgs/internal/queue/fifo"
)

func main() {
	// Example showing how to use FIFO queue with high throughput mode
	fifoExample()
}

func fifoExample() {
	// Create a new FIFO queue with high throughput mode enabled
	queue := fifo.NewQueue(true, false)

	// Set up a custom message processor
	queue.SetMessageProcessor(func(ctx context.Context, msg *common.MessageWithVisibility) (bool, error) {
		fmt.Printf("Processing message: %s (Group ID: %s, Deduplication ID: %s)\n",
			string(msg.Body),
			msg.Attributes.MessageGroupID,
			msg.Attributes.MessageDeduplicationID)
		return true, nil
	})

	// Configure the queue
	queue.SetConcurrency(5)
	queue.SetVisibilityTimeout(30 * time.Second)
	queue.Start()

	// Send messages to different message groups
	for i := 0; i < 10; i++ {
		// FIFO queues require MessageGroupID for every message
		groupID := fmt.Sprintf("group%d", i%3)

		// Create a message
		msg := common.Message{
			Attributes: common.Attributes{
				MessageGroupID: groupID,
				// Deduplication ID is optional - if not provided, content-based deduplication is used
				MessageDeduplicationID: fmt.Sprintf("msg-%d", i),
			},
			Body: []byte(fmt.Sprintf("Message %d in group %s", i, groupID)),
		}

		// Send the message
		queue.SendMessage(msg)
		log.Printf("Sent message %d to group %s", i, groupID)
	}

	// Demonstrate sending a batch of messages
	batchItems := make([]common.SendMessageBatchItem, 3)
	for i := 0; i < 3; i++ {
		batchItems[i] = common.SendMessageBatchItem{
			Message: common.Message{
				Attributes: common.Attributes{
					MessageGroupID:         "batch-group",
					MessageDeduplicationID: fmt.Sprintf("batch-msg-%d", i),
				},
				Body: []byte(fmt.Sprintf("Batch message %d", i)),
			},
			DelaySeconds: int64(i), // Add a small delay to some messages
		}
	}

	// Send the batch
	results := queue.SendMessageBatch(batchItems)
	for i, result := range results {
		log.Printf("Batch message %d send result: %v", i, result.Success)
	}

	// Wait a moment for processing to start
	time.Sleep(time.Second)

	// Receive and process messages
	for i := 0; i < 5; i++ {
		msg, receipt, ok := queue.ReceiveMessage(10 * time.Second)
		if !ok {
			log.Println("No message available")
			continue
		}

		log.Printf("Received message: %s (Group: %s)",
			string(msg.Body), msg.Attributes.MessageGroupID)

		// Delete the message explicitly (though our processor will do this automatically)
		queue.DeleteMessage(receipt)
	}

	// Show how to use receive batch
	messages, receipts, ok := queue.ReceiveMessageBatch(5, 10*time.Second)
	if ok {
		log.Printf("Received %d messages in batch", len(messages))
		for i, msg := range messages {
			log.Printf("Batch received: %s (Group: %s)",
				string(msg.Body), msg.Attributes.MessageGroupID)
			queue.DeleteMessage(receipts[i])
		}
	}

	// Try sending a duplicate message (should be deduplicated)
	dupMsg := common.Message{
		Attributes: common.Attributes{
			MessageGroupID:         "group1",
			MessageDeduplicationID: "msg-5", // This ID was already used
		},
		Body: []byte("This is a duplicate message that should be deduplicated"),
	}
	queue.SendMessage(dupMsg)
	log.Println("Sent a duplicate message with ID 'msg-5' (should be deduplicated)")

	// Demonstrate high throughput mode vs. standard mode
	log.Println("\nSwitching to standard FIFO mode (disabling high throughput)...")
	queue.DisableHighThroughputMode()

	// In standard mode, only one message per group can be in flight at a time
	for i := 0; i < 3; i++ {
		groupID := fmt.Sprintf("standard-group%d", i)
		for j := 0; j < 3; j++ {
			msg := common.Message{
				Attributes: common.Attributes{
					MessageGroupID:         groupID,
					MessageDeduplicationID: fmt.Sprintf("%s-msg-%d", groupID, j),
				},
				Body: []byte(fmt.Sprintf("Standard mode message %d in group %s", j, groupID)),
			}
			queue.SendMessage(msg)
		}
	}

	// Receive messages in standard mode - note that we'll only get one message per group
	log.Println("Receiving messages in standard mode (should get one per group):")
	messages, receipts, ok = queue.ReceiveMessageBatch(10, 10*time.Second)
	if ok {
		log.Printf("Received %d messages in standard mode batch", len(messages))
		for i, msg := range messages {
			log.Printf("Standard mode received: %s (Group: %s)",
				string(msg.Body), msg.Attributes.MessageGroupID)
			queue.DeleteMessage(receipts[i])
		}
	}

	// Enable high throughput mode again
	log.Println("\nSwitching back to high throughput mode...")
	queue.EnableHighThroughputMode()

	// In high throughput mode, we can receive multiple messages from the same group
	messages, receipts, ok = queue.ReceiveMessageBatch(10, 10*time.Second)
	if ok {
		log.Printf("Received %d messages in high throughput mode batch", len(messages))
		for i, msg := range messages {
			log.Printf("High throughput mode received: %s (Group: %s)",
				string(msg.Body), msg.Attributes.MessageGroupID)
			queue.DeleteMessage(receipts[i])
		}
	}

	// Display queue stats
	stats := queue.GetQueueStats()
	log.Println("\nQueue stats:")
	for k, v := range stats {
		log.Printf("%s: %v", k, v)
	}

	// Wait a bit for all processing to complete
	time.Sleep(2 * time.Second)

	// Clean up
	queue.Close()
	log.Println("Queue closed")
}
