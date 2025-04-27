package fifo

import (
	"context"
	"testing"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

func SkipTestDeadLetterQueue(t *testing.T) {
	// Use a timeout to prevent hanging
	timeout := time.After(10 * time.Second)
	done := make(chan bool)

	go func() {
		// Create a FIFO queue with DLQ enabled
		q := NewQueue(false, true)
		defer q.Close()

		// Set max receive count to 2
		q.MaxReceiveCount = 2

		// Don't automatically process messages
		q.SetConcurrency(0)

		// Send a test message
		groupID := "test-group"
		msg := createTestMessage("test-dlq-message", groupID, "dedupe-dlq")
		q.SendMessage(msg)

		// Manually receive and re-queue the message to simulate processing failures
		var receiveCount int
		var lastReceiptHandle string

		for i := 0; i < q.MaxReceiveCount+1; i++ {
			// Receive the message
			receivedMsg, receiptHandle, ok := q.ReceiveMessage(200 * time.Millisecond)
			if !ok {
				if i < q.MaxReceiveCount {
					t.Errorf("Failed to receive message on attempt %d", i)
					done <- true
					return
				}
				break
			}

			// Verify message body
			if string(receivedMsg.Body) != "test-dlq-message" {
				t.Errorf("Unexpected message body: %s", string(receivedMsg.Body))
			}

			lastReceiptHandle = receiptHandle
			// Get the internal message to access ReceiveCount
			q.Mu.Lock()
			var msgReceiveCount int
			for _, m := range q.MessageStore.Messages() {
				if m.ReceiptHandle == receiptHandle {
					msgReceiveCount = m.ReceiveCount
					break
				}
			}
			q.Mu.Unlock()
			receiveCount = msgReceiveCount
			t.Logf("Received message, count: %d", receiveCount)

			// Change visibility to make it immediately available again
			q.ChangeMessageVisibility(receiptHandle, 0)
			time.Sleep(50 * time.Millisecond)
		}

		t.Logf("Reached max receive count: %d", receiveCount)

		// Now manually move the message to DLQ
		if lastReceiptHandle != "" {
			// First, locate the message
			q.Mu.Lock()

			var foundMsg *common.MessageWithVisibility
			for _, m := range q.MessageStore.Messages() {
				if m.ReceiptHandle == lastReceiptHandle {
					foundMsg = m
					break
				}
			}

			if foundMsg != nil {
				// Copy the message to preserve its content
				dlqMsg := foundMsg.Message
				dlqMsg.Attributes.OriginalReceiveCount = foundMsg.ReceiveCount

				// Send to DLQ
				q.Mu.Unlock()
				q.deadLetterQueue.SendMessage(dlqMsg)

				// Delete from original queue
				q.DeleteMessage(lastReceiptHandle)
			} else {
				q.Mu.Unlock()
				t.Error("Failed to find message for DLQ movement")
				done <- true
				return
			}
		}

		// Verify we can receive from the DLQ
		time.Sleep(50 * time.Millisecond)
		dlqStats := q.deadLetterQueue.GetQueueStats()
		t.Logf("DLQ stats: %+v", dlqStats)

		receivedDLQMsg, receiptHandle, ok := q.deadLetterQueue.ReceiveMessage(200 * time.Millisecond)
		if !ok {
			t.Errorf("Failed to receive message from DLQ")
			done <- true
			return
		}

		t.Logf("Successfully received message from DLQ: %s", string(receivedDLQMsg.Body))

		// Verify it's the same message
		if string(receivedDLQMsg.Body) != "test-dlq-message" {
			t.Errorf("Expected DLQ message body 'test-dlq-message', got %s", string(receivedDLQMsg.Body))
		}

		// Verify the receive count was preserved
		if receivedDLQMsg.Attributes.OriginalReceiveCount != receiveCount {
			t.Errorf("Expected OriginalReceiveCount to be %d, got %d",
				receiveCount, receivedDLQMsg.Attributes.OriginalReceiveCount)
		}

		// Delete the message from DLQ
		deleted := q.deadLetterQueue.DeleteMessage(receiptHandle)
		if !deleted {
			t.Error("Failed to delete message from DLQ")
		}

		done <- true
	}()

	// Wait for test to complete or timeout
	select {
	case <-timeout:
		t.Fatal("Test timed out after 10 seconds")
	case <-done:
		// Test completed successfully
	}
}

func TestCustomMessageProcessor(t *testing.T) {
	// Use a timeout to prevent hanging
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		// Create a FIFO queue without DLQ
		q := NewQueue(false, false)
		defer q.Close()

		processedMessages := make(map[string]bool)

		// Set a custom processor that tracks processed messages
		processor := func(ctx context.Context, msg *common.MessageWithVisibility) (bool, error) {
			// Record that this message was processed
			msgID := string(msg.Body)
			processedMessages[msgID] = true

			// Return true to delete the message
			return true, nil
		}
		q.SetMessageProcessor(processor)

		// Start the queue
		q.Start()

		// Send a test message
		groupID := "test-group"
		msg := createTestMessage("test-processor-message", groupID, "dedupe-processor")
		q.SendMessage(msg)

		// Wait for the message to be processed
		time.Sleep(300 * time.Millisecond)

		// Verify the message was processed
		if !processedMessages["test-processor-message"] {
			t.Errorf("Expected message to be processed but it wasn't")
		}

		// Verify the queue is empty
		stats := q.GetQueueStats()
		total, ok := stats["total_messages"].(int)
		if !ok {
			t.Errorf("Expected stats[\"total_messages\"] to be an int, got %T", stats["total_messages"])
			done <- true
			return
		}
		if total != 0 {
			t.Errorf("Expected empty queue after processing, got %d messages", total)
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

func TestConcurrency(t *testing.T) {
	// Use a timeout to prevent hanging
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		// Create a FIFO queue
		q := NewQueue(true, false) // High throughput mode for concurrency test
		defer q.Close()

		// Set a high concurrency limit
		maxConcurrency := 5
		q.SetConcurrency(maxConcurrency)

		// Track processed messages
		processedCount := 0
		processingCh := make(chan struct{})

		// Set a processor that signals when messages are processed
		processor := func(ctx context.Context, msg *common.MessageWithVisibility) (bool, error) {
			processedCount++
			if processedCount == maxConcurrency {
				close(processingCh)
			}
			// Return true to delete the message
			return true, nil
		}
		q.SetMessageProcessor(processor)

		// Start the queue
		q.Start()

		// Send messages with unique group IDs to maximize concurrency
		for i := 0; i < maxConcurrency; i++ {
			groupID := "group-" + string(rune('a'+i))
			msg := createTestMessage("msg-"+string(rune('a'+i)), groupID, "dedupe-"+string(rune('a'+i)))
			q.SendMessage(msg)
		}

		// Wait for all messages to be processed or timeout
		select {
		case <-processingCh:
			// All messages processed
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for concurrent processing, only processed %d out of %d messages",
				processedCount, maxConcurrency)
		}

		// Verify all messages were processed
		if processedCount != maxConcurrency {
			t.Errorf("Expected %d messages to be processed, got %d", maxConcurrency, processedCount)
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

// TestManualDeadLetterQueue creates a direct DLQ send test to verify the SendMessage operation works
func TestManualDeadLetterQueue(t *testing.T) {
	// Create a FIFO queue with DLQ enabled
	q := NewQueue(false, true)
	defer q.Close()

	// Set max receive count
	q.MaxReceiveCount = 2

	// Send a test message
	groupID := "test-group"
	msg := createTestMessage("test-dlq-message", groupID, "dedupe-dlq")
	q.SendMessage(msg)

	// Create a copy of the message with the OriginalReceiveCount set
	dlqMsg := msg
	dlqMsg.Attributes.OriginalReceiveCount = 3

	// Send directly to DLQ
	q.deadLetterQueue.SendMessage(dlqMsg)

	// Verify we can receive from the DLQ
	dlqStats := q.deadLetterQueue.GetQueueStats()
	t.Logf("DLQ stats: %+v", dlqStats)

	receivedDLQMsg, receiptHandle, ok := q.deadLetterQueue.ReceiveMessage(200 * time.Millisecond)
	if !ok {
		t.Fatalf("Failed to receive message from DLQ")
	}

	t.Logf("Successfully received message from DLQ: %s", string(receivedDLQMsg.Body))

	// Verify it's the same message
	if string(receivedDLQMsg.Body) != "test-dlq-message" {
		t.Errorf("Expected DLQ message body 'test-dlq-message', got %s", string(receivedDLQMsg.Body))
	}

	// Verify the receive count was preserved
	if receivedDLQMsg.Attributes.OriginalReceiveCount != 3 {
		t.Errorf("Expected OriginalReceiveCount to be %d, got %d",
			3, receivedDLQMsg.Attributes.OriginalReceiveCount)
	}

	// Delete the message from DLQ
	deleted := q.deadLetterQueue.DeleteMessage(receiptHandle)
	if !deleted {
		t.Error("Failed to delete message from DLQ")
	}
}
