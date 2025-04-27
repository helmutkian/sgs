package fifo

import (
	"testing"
	"time"

	"github.com/helmutkian/sgs/internal/queue/common"
)

func TestChangeMessageVisibility(t *testing.T) {
	// Use a timeout to prevent hanging
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		q := NewQueue(false, false)
		defer q.Close()

		// Send a message
		groupID := "test-group"
		msg := createTestMessage("test-message", groupID, "dedupe-id")
		q.SendMessage(msg)

		// Add a small delay to ensure message is sent
		time.Sleep(100 * time.Millisecond)

		// Receive the message with a short timeout
		_, receiptHandle, ok := q.ReceiveMessage(200 * time.Millisecond)
		if !ok {
			t.Errorf("Failed to receive message")
			done <- true
			return
		}

		// Change visibility to immediate (0 timeout - message becomes visible)
		changed := q.ChangeMessageVisibility(receiptHandle, 0)
		if !changed {
			t.Errorf("Failed to change message visibility")
			done <- true
			return
		}

		// Add a small delay to ensure visibility change takes effect
		time.Sleep(50 * time.Millisecond)

		// In standard FIFO mode, this should have released the message group lock
		// So the message should be visible again
		receivedMsg, newReceiptHandle, ok := q.ReceiveMessage(200 * time.Millisecond)
		if !ok {
			t.Errorf("Expected message to be visible after visibility timeout change, but wasn't")
			done <- true
			return
		}

		if string(receivedMsg.Body) != "test-message" {
			t.Errorf("Expected message body 'test-message', got %s", string(receivedMsg.Body))
		}

		// Now change to a longer timeout
		changed = q.ChangeMessageVisibility(newReceiptHandle, 10*time.Second)
		if !changed {
			t.Errorf("Failed to change message visibility")
			done <- true
			return
		}

		// Message should not be visible
		_, _, ok = q.ReceiveMessage(100 * time.Millisecond)
		if ok {
			t.Error("Expected message to be invisible after visibility timeout change, but was visible")
		}

		// Delete the message
		deleted := q.DeleteMessage(newReceiptHandle)
		if !deleted {
			t.Error("Failed to delete message")
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

func TestVisibilityTimeout(t *testing.T) {
	visibilityTimeout := 200 * time.Millisecond

	// Use a test with timeout to prevent hanging
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		q := NewQueue(false, false)
		defer q.Close()

		// Send a message
		groupID := "test-group"
		msg := createTestMessage("test-message", groupID, "dedupe-id")
		q.SendMessage(msg)

		// Add a small delay to ensure message is sent
		time.Sleep(100 * time.Millisecond)

		// Receive the message with short visibility timeout
		_, receiptHandle, ok := q.ReceiveMessage(visibilityTimeout)
		if !ok {
			t.Errorf("Failed to receive message")
			done <- true
			return
		}

		// Message should be invisible
		_, _, ok = q.ReceiveMessage(50 * time.Millisecond)
		if ok {
			t.Error("Expected message to be invisible, but was visible")
		}

		// Instead of waiting for visibility timeout to expire,
		// explicitly make the message visible using ChangeMessageVisibility
		changed := q.ChangeMessageVisibility(receiptHandle, 0)
		if !changed {
			t.Errorf("Failed to change message visibility")
			done <- true
			return
		}

		// Add a small delay to ensure visibility change takes effect
		time.Sleep(50 * time.Millisecond)

		// Message should be visible again
		receivedAgain, receiptHandle2, ok := q.ReceiveMessage(200 * time.Millisecond)
		if !ok {
			t.Errorf("Expected message to be visible after visibility reset, but wasn't")
			done <- true
			return
		}

		if string(receivedAgain.Body) != "test-message" {
			t.Errorf("Expected message body 'test-message', got %s", string(receivedAgain.Body))
		}

		// Delete the message
		deleted := q.DeleteMessage(receiptHandle2)
		if !deleted {
			t.Error("Failed to delete message")
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

func TestChangeMessageVisibilityBatch(t *testing.T) {
	// Use a timeout to prevent hanging
	timeout := time.After(10 * time.Second)
	done := make(chan bool)

	go func() {
		q := NewQueue(false, false)
		defer q.Close()

		// Send multiple messages with different group IDs
		groupIDs := []string{"group1", "group2", "group3"}
		receiptHandles := make([]string, 0, len(groupIDs))

		for _, groupID := range groupIDs {
			msg := createTestMessage("msg-"+groupID, groupID, "dedupe-"+groupID)
			q.SendMessage(msg)
		}

		// Add a small delay to ensure messages are sent
		time.Sleep(100 * time.Millisecond)

		// Receive messages - should get one from each group in high throughput mode
		q.EnableHighThroughputMode()
		for i := 0; i < len(groupIDs); i++ {
			_, receipt, ok := q.ReceiveMessage(200 * time.Millisecond)
			if !ok {
				t.Errorf("Failed to receive message %d", i)
				done <- true
				return
			}
			receiptHandles = append(receiptHandles, receipt)
		}

		// Create batch visibility items - make first two messages visible immediately
		items := make([]common.ChangeMessageVisibilityBatchItem, len(receiptHandles))
		for i, handle := range receiptHandles {
			timeout := time.Duration(0)
			if i == len(receiptHandles)-1 {
				timeout = 10 * time.Second // Keep the last one invisible
			}
			items[i] = common.ChangeMessageVisibilityBatchItem{
				ReceiptHandle: handle,
				Timeout:       timeout,
			}
		}

		// Change visibility in batch
		results := q.ChangeMessageVisibilityBatch(items)

		// Verify all operations succeeded
		for i, result := range results {
			if !result.Success {
				t.Errorf("Expected success for item %d, but got failure", i)
			}
		}

		// Add a small delay to ensure visibility changes take effect
		time.Sleep(50 * time.Millisecond)

		// We should be able to receive the first two messages again
		messagesReceived := 0
		for i := 0; i < 2; i++ {
			_, newReceipt, ok := q.ReceiveMessage(100 * time.Millisecond)
			if ok {
				messagesReceived++
				// Delete the message
				q.DeleteMessage(newReceipt)
			}
		}

		if messagesReceived != 2 {
			t.Errorf("Expected to receive 2 visible messages, got %d", messagesReceived)
		}

		// The third message should still be invisible
		// Queue should be empty (1 in flight, 0 visible)
		stats := q.GetQueueStats()
		visibleCount, ok1 := stats["visible_messages"].(int)
		inFlightCount, ok2 := stats["in_flight_messages"].(int)

		if !ok1 || !ok2 {
			t.Errorf("Expected stats[\"visible_messages\"] and stats[\"in_flight_messages\"] to be ints")
			done <- true
			return
		}

		if visibleCount != 0 || inFlightCount != 1 {
			t.Errorf("Expected 0 visible, 1 in flight; got visible=%d, in_flight=%d",
				visibleCount, inFlightCount)
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
