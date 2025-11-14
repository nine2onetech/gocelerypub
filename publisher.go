package gocelerypub

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Publisher publishes Celery tasks to a message broker.
// It maintains a connection to the broker and handles message serialization.
type Publisher struct {
	broker   Broker
	config   Config
	taskChan chan *internalPublishRequest // Channel for channel-based mode (nil in direct mode)
}

// PublishRequest represents a request to publish a Celery task.
type PublishRequest struct {
	Queue  string                 // Target queue name
	Task   string                 // Task name (e.g., "tasks.add")
	Args   []interface{}          // Positional arguments for the task
	Kwargs map[string]interface{} // Keyword arguments for the task
}

// internalPublishRequest is used internally for channel-based publishing.
// It wraps a PublishRequest with an error channel for response communication.
type internalPublishRequest struct {
	Request *PublishRequest
	ErrCh   chan error // Buffered channel to receive the publish result
}

// BrokerType represents the type of message broker to use.
type BrokerType string

// Supported broker types
var (
	AMQP BrokerType = "amqp" // RabbitMQ/AMQP broker
)

// PublishMode represents the publishing mode for the Publisher.
type PublishMode string

// Supported publish modes
var (
	// DirectMode publishes messages directly without goroutine safety.
	// Use this mode for simple, single-threaded applications.
	// WARNING: Not safe for concurrent use from multiple goroutines.
	DirectMode PublishMode = "direct"

	// ChannelMode publishes messages through an internal task channel.
	// This mode is goroutine-safe and suitable for high-performance concurrent applications.
	// Uses a single dedicated goroutine to handle all AMQP operations sequentially.
	ChannelMode PublishMode = "channel"
)

// Config holds the configuration for a Publisher.
type Config struct {
	BrokerType             BrokerType  // Type of broker to use (e.g., AMQP)
	HostURL                string      // Broker connection URL (e.g., "amqp://guest:guest@localhost:5672/")
	PublishMode            PublishMode // Publishing mode (DirectMode or ChannelMode)
	messageProtocolVersion int         // Celery message protocol version (currently fixed to 1)
}

// New creates a new Publisher with the given configuration.
// It initializes the appropriate broker based on the BrokerType specified in the config.
// If PublishMode is not specified, it defaults to DirectMode.
// For ChannelMode, it starts a dedicated goroutine to handle publishing operations.
// Returns an error if the broker type is not supported.
func New(cfg Config) (*Publisher, error) {
	cfg.messageProtocolVersion = 1 // Currently only protocol version 1 is supported

	// Default to DirectMode if not specified
	if cfg.PublishMode == "" {
		cfg.PublishMode = DirectMode
	}

	// Validate PublishMode
	if cfg.PublishMode != DirectMode && cfg.PublishMode != ChannelMode {
		return nil, fmt.Errorf("unsupported publish mode: %s", cfg.PublishMode)
	}

	// Create broker instance based on BrokerType
	var broker Broker
	switch cfg.BrokerType {
	case AMQP:
		broker = NewAMQPBroker(cfg.HostURL)
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", cfg.BrokerType)
	}

	p := &Publisher{
		broker: broker,
		config: cfg,
	}

	// Initialize channel-based mode if requested
	if cfg.PublishMode == ChannelMode {
		p.taskChan = make(chan *internalPublishRequest)
		go p.run() // Start the dedicated publishing goroutine
	}

	return p, nil
}

// handleRequest handles the actual publish logic with connection management and retry logic.
// It attempts to reconnect if the broker connection is lost and retries once on PRECONDITION_FAILED errors.
func (p *Publisher) handleRequest(req *PublishRequest) error {
	// Reconnect if the broker connection is lost
	if !p.broker.CanPublish() {
		if err := p.broker.Reconnect(); err != nil {
			return fmt.Errorf("reconnect failed: %w", err)
		}
	}

	// Attempt to publish the message
	err := p.Publish(req)

	// Retry once on PRECONDITION_FAILED error (e.g., queue settings mismatch)
	if err != nil && strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		if retryErr := p.broker.Reconnect(); retryErr != nil {
			return fmt.Errorf("reconnect after PRECONDITION_FAILED failed: %w", retryErr)
		}

		// Retry the publish operation
		err = p.Publish(req)
	}

	return err
}

// Publish publishes a Celery task to the specified queue.
// It creates a Celery-compatible message and sends it via the configured broker.
// The message uses the default exchange ("") and routes directly to the queue by name.
func (p *Publisher) Publish(req *PublishRequest) error {
	// Create TaskMessage from the request
	taskMsg, _ := p.generateTaskMessage(req)

	// Convert to CeleryMessage with delivery information
	msg := taskMsg.ToCeleryMessage(CeleryDeliveryInfo{
		RoutingKey: req.Queue, // Route to queue by name (using default exchange)
		Exchange:   "",        // Empty string means default exchange
	})

	// Send via broker
	err := p.broker.SendCeleryMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	return nil
}

// generateTaskMessage creates a TaskMessage from a PublishRequest.
// It generates a unique ID for the task and populates it with the request data.
func (p *Publisher) generateTaskMessage(req *PublishRequest) (TaskMessage, error) {
	taskMsg := &TaskMessageV1{
		ID:     uuid.New().String(),
		Task:   req.Task,
		Args:   req.Args,
		Kwargs: req.Kwargs,
	}

	return taskMsg, nil
}

// run processes publish requests from the task channel.
// It handles reconnection when the broker connection is lost and retries on PRECONDITION_FAILED errors.
// This method runs in a dedicated goroutine when using ChannelMode.
func (p *Publisher) run() {
	for req := range p.taskChan {
		// Handle the publish request with automatic reconnection and retry logic
		err := p.handleRequest(req.Request)

		// Send the result back to the caller
		req.ErrCh <- err
	}
}

// Send publishes a Celery task using channel-based mode.
// This method is goroutine-safe and should be used when the Publisher is configured with ChannelMode.
// It sends the publish request to an internal channel where a dedicated goroutine handles the actual publishing.
//
// The error channel is buffered to prevent blocking in the run() goroutine.
// Even if this method returns before the caller reads from errCh, the run() goroutine won't block.
//
// Returns an error if publishing fails or if the Publisher is not in ChannelMode.
func (p *Publisher) Send(req *PublishRequest) error {
	if p.taskChan == nil {
		return fmt.Errorf("send() can only be used in channel mode")
	}

	// Create a buffered error channel to receive the publish result.
	// Buffer size of 1 prevents blocking in run() if this goroutine hasn't reached
	// the receive operation (<-errCh) yet when run() sends the error.
	errCh := make(chan error, 1)

	// Send the request to the task channel
	p.taskChan <- &internalPublishRequest{
		Request: req,
		ErrCh:   errCh,
	}

	// Wait for the result from the run() goroutine
	err := <-errCh
	close(errCh) // Clean up the one-time error channel

	return err
}

// Close gracefully shuts down the Publisher.
// For ChannelMode, it closes the task channel and waits for the run() goroutine to finish.
// For both modes, it closes the broker connection.
// This method should be called when the Publisher is no longer needed to prevent resource leaks.
func (p *Publisher) Close() error {
	// Close the task channel if in ChannelMode
	// This will cause the run() goroutine to exit when it finishes processing pending tasks
	if p.taskChan != nil {
		close(p.taskChan)
		// Note: We don't wait for the goroutine to finish here.
		// The goroutine will exit when it drains the channel.
	}

	// Close broker connection if it's still open
	if amqpBroker, ok := p.broker.(*AMQPBroker); ok {
		if !amqpBroker.isClosed() {
			if err := amqpBroker.Channel.Close(); err != nil {
				return fmt.Errorf("failed to close channel: %w", err)
			}
			if err := amqpBroker.Connection.Close(); err != nil {
				return fmt.Errorf("failed to close connection: %w", err)
			}
		}
	}

	return nil
}
