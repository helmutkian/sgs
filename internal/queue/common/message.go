package common

import "time"

// Message represents a queue message
type Message struct {
	Attributes Attributes `json:"Attributes"`
	Body       []byte     `json:"Body"`
}

// Attributes contains SQS-specific message attributes
type Attributes struct {
	MessageID              string `json:"MessageId"`
	MessageGroupID         string `json:"MessageGroupId"`
	MessageDeduplicationID string `json:"MessageDeduplicationId"`
	OriginalReceiveCount   int    `json:"OriginalReceiveCount"`
	DeduplicationID        string `json:"DeduplicationId"`
	SequenceNumber         string `json:"SequenceNumber"`
}

// MessageStatus represents the status of a message in the queue
type MessageStatus int

const (
	_ MessageStatus = iota
	// MessageVisible indicates the message is visible and can be received
	MessageVisible
	// MessageInFlight indicates the message has been received but not yet deleted or returned
	MessageInFlight
	// MessageProcessing indicates the message is currently being processed (FIFO specific)
	MessageProcessing
)

// MessageWithVisibility wraps a message with its visibility information
type MessageWithVisibility struct {
	Message
	Status          MessageStatus // Current message status
	ReceiptHandle   string        // Unique identifier for this receipt of the message
	InvisibleUntil  time.Time     // Time when the message becomes visible again
	VisibilityTimer *time.Timer   // Timer for visibility timeout
	ReceiveCount    int           // Number of times this message has been received
	DelayedUntil    time.Time     // Time when the message becomes available (if delayed)
	DelayTimer      *time.Timer   // Timer for message delay
}

// Helper functions for common message operations

// IsVisible checks if a message is currently visible
func (m *MessageWithVisibility) IsVisible() bool {
	return m.Status == MessageVisible && m.DelayedUntil.IsZero()
}

// IsInFlight checks if a message is in flight
func (m *MessageWithVisibility) IsInFlight() bool {
	return m.Status == MessageInFlight
}

// IsDelayed checks if a message is delayed
func (m *MessageWithVisibility) IsDelayed() bool {
	return !m.DelayedUntil.IsZero() && time.Now().Before(m.DelayedUntil)
}

// IncrementReceiveCount increments the receive count and returns the new value
func (m *MessageWithVisibility) IncrementReceiveCount() int {
	m.ReceiveCount++
	return m.ReceiveCount
}

// SetVisibilityTimeout sets a new visibility timeout for the message
func (m *MessageWithVisibility) SetVisibilityTimeout(timeout time.Duration) {
	// Stop any existing timer
	if m.VisibilityTimer != nil {
		m.VisibilityTimer.Stop()
	}

	m.Status = MessageInFlight
	m.InvisibleUntil = time.Now().Add(timeout)
}

// ClearVisibilityTimeout makes the message immediately visible
func (m *MessageWithVisibility) ClearVisibilityTimeout() {
	// Stop any existing timer
	if m.VisibilityTimer != nil {
		m.VisibilityTimer.Stop()
	}

	m.Status = MessageVisible
	m.InvisibleUntil = time.Time{}
}

// SetDelay sets a delay for the message
func (m *MessageWithVisibility) SetDelay(delaySeconds int64) {
	// Stop any existing timer
	if m.DelayTimer != nil {
		m.DelayTimer.Stop()
	}

	if delaySeconds > 0 {
		m.Status = MessageInFlight // Use InFlight for delayed messages
		m.DelayedUntil = time.Now().Add(time.Duration(delaySeconds) * time.Second)
	} else {
		m.DelayedUntil = time.Time{}
	}
}
