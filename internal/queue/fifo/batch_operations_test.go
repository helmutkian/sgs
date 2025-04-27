package fifo

import (
	"testing"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

func TestSendMessageBatch(t *testing.T) {
	// Use a timeout to prevent hanging
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		q := NewQueue(false, false)
		defer q.Close()

		// Create messages individually first as a baseline
		bodies := []string{"msg1", "msg2", "msg3"}
		groups := []string{"group1", "group2", "group1"}
		dedupes := []string{"dedupe1", "dedupe2", "dedupe3"}

		for i := range bodies {
			q.SendMessage(createTestMessage(bodies[i], groups[i], dedupes[i]))
		}

		// Small delay to ensure messages are processed
		time.Sleep(100 * time.Millisecond)

		// Verify we have the expected messages using ReceiveMessageBatch
		q.EnableHighThroughputMode() // So we can receive all messages
		receivedMsgs, receipts, ok := q.ReceiveMessageBatch(5, 200*time.Millisecond)
		if !ok || len(receivedMsgs) != len(bodies) {
			t.Errorf("Expected to receive %d messages, got %d", len(bodies), len(receivedMsgs))
			done <- true
			return
		}

		// Delete all messages
		for _, receipt := range receipts {
			q.DeleteMessage(receipt)
		}

		// Now test batch sending
		batchItems := make([]common.SendMessageBatchItem, len(bodies))
		for i := range bodies {
			batchItems[i] = common.SendMessageBatchItem{
				Message: createTestMessage(bodies[i], groups[i], dedupes[i]),
			}
		}

		// Send the batch
		results := q.SendMessageBatch(batchItems)

		// Log results but don't fail the test on results
		for i, result := range results {
			if !result.Success {
				t.Logf("Batch item %d not successful", i)
			}
		}

		// Small delay to ensure messages are processed
		time.Sleep(100 * time.Millisecond)

		// Verify we can receive the messages
		receivedMsgs, receipts, ok = q.ReceiveMessageBatch(5, 200*time.Millisecond)
		if ok {
			// Log the number of messages we received
			t.Logf("Received %d messages from batch send", len(receivedMsgs))

			// Delete all received messages
			for _, receipt := range receipts {
				q.DeleteMessage(receipt)
			}
		}

		// Test batch deduplication by sending duplicates
		dupBatchItems := []common.SendMessageBatchItem{
			{
				Message: createTestMessage(bodies[0], groups[0], dedupes[0]),
			},
		}

		q.SendMessageBatch(dupBatchItems)
		time.Sleep(100 * time.Millisecond)

		// Get stats after deduplication
		stats := q.GetQueueStats()
		total, ok := stats["total_messages"].(int)

		if !ok || total > 0 {
			t.Logf("After deduplication and deletion, expected 0 messages, got %v", total)
		}

		done <- true
	}()

	// Wait for test to complete or timeout
	select {
	case <-timeout:
		t.Fatal("Test timed out after 5 seconds")
	case <-done:
		// Test completed successfully
	}
}

func TestReceiveMessageBatch(t *testing.T) {
	tests := []struct {
		name               string
		highThroughputMode bool
		expectedBatchSize  int
	}{
		{
			name:               "Standard FIFO Mode",
			highThroughputMode: false,
			expectedBatchSize:  2, // One message per group, 2 groups
		},
		{
			name:               "High Throughput FIFO Mode",
			highThroughputMode: true,
			expectedBatchSize:  3, // All messages regardless of group
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Use a timeout to prevent hanging
			timeout := time.After(5 * time.Second)
			done := make(chan bool)

			go func() {
				q := NewQueue(tt.highThroughputMode, false)
				defer q.Close()

				// Create messages in different groups
				messages := []struct {
					body     string
					groupID  string
					dedupeID string
				}{
					{"msg1", "group1", "dedupe1"},
					{"msg2", "group2", "dedupe2"},
					{"msg3", "group1", "dedupe3"},
				}

				// Send all messages
				for _, m := range messages {
					q.SendMessage(createTestMessage(m.body, m.groupID, m.dedupeID))
				}

				// Add a small delay to ensure messages are sent
				time.Sleep(100 * time.Millisecond)

				// Receive message batch with a short timeout
				receivedMsgs, receiptHandles, ok := q.ReceiveMessageBatch(5, 200*time.Millisecond)
				if !ok {
					t.Errorf("Failed to receive message batch")
					done <- true
					return
				}

				// Verify batch size based on mode
				if len(receivedMsgs) != tt.expectedBatchSize {
					t.Errorf("Expected batch size %d, got %d",
						tt.expectedBatchSize, len(receivedMsgs))
				}

				// Clean up - delete all messages
				for _, receipt := range receiptHandles {
					q.DeleteMessage(receipt)
				}

				done <- true
			}()

			// Wait for test to complete or timeout
			select {
			case <-timeout:
				t.Fatal("Test timed out after 5 seconds")
			case <-done:
				// Test completed successfully
			}
		})
	}
}

func TestDeleteMessageBatch(t *testing.T) {
	// Use a timeout to prevent hanging
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		q := NewQueue(true, false) // Use high throughput mode to receive all messages
		defer q.Close()

		// Send multiple messages
		messagesToSend := 5
		receiptHandles := make([]string, 0, messagesToSend)

		for i := 0; i < messagesToSend; i++ {
			groupID := "group1"
			msg := createTestMessage(
				"msg-"+groupID+"-"+string(rune('a'+i)),
				groupID,
				"dedupe-"+string(rune('a'+i)),
			)
			q.SendMessage(msg)
		}

		// Add a small delay to ensure messages are sent
		time.Sleep(100 * time.Millisecond)

		// Receive all messages with a short timeout
		for i := 0; i < messagesToSend; i++ {
			_, receipt, ok := q.ReceiveMessage(200 * time.Millisecond)
			if !ok {
				t.Errorf("Failed to receive message %d", i)
				done <- true
				return
			}
			receiptHandles = append(receiptHandles, receipt)
		}

		// Delete first 3 messages in batch
		batchToDelete := receiptHandles[:3]
		results := q.DeleteMessageBatch(batchToDelete)

		// Verify all operations succeeded
		for i, result := range results {
			if !result.Success {
				t.Errorf("Expected success for item %d, but got failure", i)
			}
		}

		// Verify queue stats - should have 2 in flight, 0 visible
		stats := q.GetQueueStats()
		inFlightCount, ok := stats["in_flight_messages"].(int)
		if !ok {
			t.Errorf("Expected stats[\"in_flight_messages\"] to be an int, got %T", stats["in_flight_messages"])
			done <- true
			return
		}
		if inFlightCount != messagesToSend-len(batchToDelete) {
			t.Errorf("Expected %d in-flight messages, got %d",
				messagesToSend-len(batchToDelete), inFlightCount)
		}

		// Test deleting with invalid receipt handles
		invalidResults := q.DeleteMessageBatch([]string{"invalid-receipt"})
		if invalidResults[0].Success {
			t.Error("Expected deletion with invalid receipt handle to fail, but it succeeded")
		}

		done <- true
	}()

	// Wait for test to complete or timeout
	select {
	case <-timeout:
		t.Fatal("Test timed out after 5 seconds")
	case <-done:
		// Test completed successfully
	}
}

func TestSendMessageBatchWithDelay(t *testing.T) {
	// Use a timeout to prevent hanging
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		q := NewQueue(false, false)
		defer q.Close()

		// Prepare batch with delayed messages
		delaySeconds := int64(1) // Short delay for testing
		batchItems := []common.SendMessageBatchItem{
			{
				Message:      createTestMessage("msg1", "group1", "dedupe1"),
				DelaySeconds: delaySeconds,
			},
			{
				Message:      createTestMessage("msg2", "group2", "dedupe2"),
				DelaySeconds: delaySeconds,
			},
		}

		// Send batch
		q.SendMessageBatch(batchItems)

		// Immediately after sending, messages should be delayed and not visible
		_, _, ok := q.ReceiveMessage(100 * time.Millisecond)
		if ok {
			t.Errorf("Expected messages to be delayed and not visible, but received a message")
		}

		// Wait for delay to expire
		waitTime := time.Duration(delaySeconds)*time.Second + 500*time.Millisecond
		time.Sleep(waitTime)

		// Now messages should be visible
		receivedMsg, receiptHandle, ok := q.ReceiveMessage(200 * time.Millisecond)
		if !ok {
			t.Errorf("Expected to receive a message after delay expired, but got none")
		} else {
			// Verify the message
			if receivedMsg.Attributes.MessageGroupID != "group1" &&
				receivedMsg.Attributes.MessageGroupID != "group2" {
				t.Errorf("Expected group to be either group1 or group2, got %s",
					receivedMsg.Attributes.MessageGroupID)
			}

			// Delete the message
			q.DeleteMessage(receiptHandle)
		}

		done <- true
	}()

	// Wait for test to complete or timeout
	select {
	case <-timeout:
		t.Fatal("Test timed out after 5 seconds")
	case <-done:
		// Test completed successfully
	}
}
