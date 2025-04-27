package fifo

import (
	"testing"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

func TestNewQueue(t *testing.T) {
	tests := []struct {
		name              string
		highThroughput    bool
		dlqEnabled        bool
		wantDeadLetterPtr bool
	}{
		{
			name:              "Standard FIFO with no DLQ",
			highThroughput:    false,
			dlqEnabled:        false,
			wantDeadLetterPtr: false,
		},
		{
			name:              "High throughput FIFO with no DLQ",
			highThroughput:    true,
			dlqEnabled:        false,
			wantDeadLetterPtr: false,
		},
		{
			name:              "Standard FIFO with DLQ",
			highThroughput:    false,
			dlqEnabled:        true,
			wantDeadLetterPtr: true,
		},
		{
			name:              "High throughput FIFO with DLQ",
			highThroughput:    true,
			dlqEnabled:        true,
			wantDeadLetterPtr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQueue(tt.highThroughput, tt.dlqEnabled)
			if q == nil {
				t.Fatal("Expected queue to be created, got nil")
			}

			if tt.highThroughput != q.HighThroughputMode {
				t.Errorf("Expected HighThroughputMode = %v, got %v", tt.highThroughput, q.HighThroughputMode)
			}

			if (q.deadLetterQueue != nil) != tt.wantDeadLetterPtr {
				t.Errorf("Expected deadLetterQueue presence = %v, got %v", tt.wantDeadLetterPtr, q.deadLetterQueue != nil)
			}

			// Clean up
			q.Close()
		})
	}
}

func TestQueueLifecycle(t *testing.T) {
	// Create a queue with default settings
	q := NewQueue(false, false)
	defer q.Close()

	// Test that the queue is initialized properly
	if q.DeduplicationWindow != DefaultDeduplicationTimeWindow {
		t.Errorf("Expected DeduplicationWindow = %v, got %v", DefaultDeduplicationTimeWindow, q.DeduplicationWindow)
	}

	if q.HighThroughputMode != false {
		t.Errorf("Expected HighThroughputMode = false, got %v", q.HighThroughputMode)
	}

	if q.ContentBasedDeduplication != false {
		t.Errorf("Expected ContentBasedDeduplication = false, got %v", q.ContentBasedDeduplication)
	}

	// Test changing settings
	q.SetDeduplicationWindow(time.Hour)
	if q.DeduplicationWindow != time.Hour {
		t.Errorf("Expected DeduplicationWindow = %v, got %v", time.Hour, q.DeduplicationWindow)
	}

	q.EnableHighThroughputMode()
	if !q.HighThroughputMode {
		t.Errorf("Expected HighThroughputMode = true, got %v", q.HighThroughputMode)
	}

	q.DisableHighThroughputMode()
	if q.HighThroughputMode {
		t.Errorf("Expected HighThroughputMode = false, got %v", q.HighThroughputMode)
	}

	q.SetContentBasedDeduplication(true)
	if !q.ContentBasedDeduplication {
		t.Errorf("Expected ContentBasedDeduplication = true, got %v", q.ContentBasedDeduplication)
	}
}

func createTestMessage(body string, groupID string, deduplicationID string) common.Message {
	return common.Message{
		Body: []byte(body),
		Attributes: common.Attributes{
			MessageID:              "msg-" + body,
			MessageGroupID:         groupID,
			MessageDeduplicationID: deduplicationID,
		},
	}
}

func TestSendAndReceiveMessage(t *testing.T) {
	tests := []struct {
		name               string
		highThroughputMode bool
	}{
		{
			name:               "Standard FIFO Mode",
			highThroughputMode: false,
		},
		{
			name:               "High Throughput FIFO Mode",
			highThroughputMode: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a test with timeout to prevent hanging
			t.Parallel()
			timeout := time.After(5 * time.Second)
			done := make(chan bool)

			go func() {
				q := NewQueue(tt.highThroughputMode, false)
				defer q.Close()

				// Create and send a test message
				groupID := "test-group"
				msg := createTestMessage("test-message", groupID, "dedupe-id")
				q.SendMessage(msg)

				// Small delay to ensure message is available
				time.Sleep(50 * time.Millisecond)

				// Receive the message with a short timeout
				receivedMsg, receiptHandle, ok := q.ReceiveMessage(200 * time.Millisecond)
				if !ok {
					t.Errorf("Expected to receive a message, got none")
					done <- true
					return
				}

				if string(receivedMsg.Body) != string(msg.Body) {
					t.Errorf("Expected message body %s, got %s", string(msg.Body), string(receivedMsg.Body))
				}

				if receivedMsg.Attributes.MessageGroupID != groupID {
					t.Errorf("Expected MessageGroupID %s, got %s", groupID, receivedMsg.Attributes.MessageGroupID)
				}

				// Verify message is not visible (is in flight)
				_, _, ok = q.ReceiveMessage(100 * time.Millisecond)
				if ok {
					t.Error("Received same message twice, expected it to be invisible after first receive")
				}

				// Delete the message
				deleted := q.DeleteMessage(receiptHandle)
				if !deleted {
					t.Error("Failed to delete message")
				}

				// Verify queue is empty
				stats := q.GetQueueStats()
				visibleCount, ok1 := stats["visible_messages"].(int)
				inFlightCount, ok2 := stats["in_flight_messages"].(int)

				if !ok1 || !ok2 {
					t.Errorf("Expected stats[\"visible_messages\"] and stats[\"in_flight_messages\"] to be ints")
				}

				if visibleCount != 0 || inFlightCount != 0 {
					t.Errorf("Expected empty queue, got visible=%d, in_flight=%d", visibleCount, inFlightCount)
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

func TestMessageDeduplication(t *testing.T) {
	// Use a test with timeout to prevent hanging
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		q := NewQueue(false, false)
		defer q.Close()

		// Send a message
		groupID := "test-group"
		dedupeID := "dedupe-id"
		msg := createTestMessage("test-message", groupID, dedupeID)
		q.SendMessage(msg)

		// Small delay to ensure message is available
		time.Sleep(50 * time.Millisecond)

		// Send the exact same message again with same deduplication ID
		q.SendMessage(msg)

		// Get stats - should only see one message
		stats := q.GetQueueStats()
		total, ok := stats["total_messages"].(int)
		if !ok {
			t.Errorf("Expected stats[\"total_messages\"] to be an int, got %T", stats["total_messages"])
			done <- true
			return
		}
		if total != 1 {
			t.Errorf("Expected 1 message after deduplication, got %d", total)
		}

		// Receive and delete the message
		_, receiptHandle, ok := q.ReceiveMessage(200 * time.Millisecond)
		if !ok {
			t.Errorf("Failed to receive message")
			done <- true
			return
		}
		q.DeleteMessage(receiptHandle)

		// Force remove the deduplication ID instead of waiting for window expiration
		q.ForceRemoveDeduplicationID(dedupeID)

		// Send the message again - should be accepted now
		q.SendMessage(msg)

		// Small delay to ensure message is processed
		time.Sleep(50 * time.Millisecond)

		stats = q.GetQueueStats()
		total, ok = stats["total_messages"].(int)
		if !ok {
			t.Errorf("Expected stats[\"total_messages\"] to be an int, got %T", stats["total_messages"])
			done <- true
			return
		}
		if total != 1 {
			t.Errorf("Expected 1 message after deduplication window expiration, got %d", total)
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

func TestFIFOOrdering(t *testing.T) {
	// Use a test with timeout to prevent hanging
	timeout := time.After(10 * time.Second)
	done := make(chan bool)

	go func() {
		q := NewQueue(false, false)
		defer q.Close()

		// Send messages in a specific order
		groupID := "test-group"
		messages := []string{"first", "second", "third", "fourth", "fifth"}

		for _, m := range messages {
			msg := createTestMessage(m, groupID, m+"-dedupe")
			q.SendMessage(msg)
		}

		// Small delay to ensure all messages are available
		time.Sleep(100 * time.Millisecond)

		// Verify they come out in the same order
		for i, expected := range messages {
			receivedMsg, receiptHandle, ok := q.ReceiveMessage(200 * time.Millisecond)
			if !ok {
				t.Errorf("Failed to receive message %d", i)
				done <- true
				return
			}

			if string(receivedMsg.Body) != expected {
				t.Errorf("Expected message %d to be %q, got %q", i, expected, string(receivedMsg.Body))
			}

			// Delete the message
			deleted := q.DeleteMessage(receiptHandle)
			if !deleted {
				t.Errorf("Failed to delete message %d", i)
			}

			// Small delay between receives
			time.Sleep(50 * time.Millisecond)
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
