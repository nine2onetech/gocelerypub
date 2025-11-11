package go_celery_publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RabbitMQTestContainer represents a RabbitMQ test container
type RabbitMQTestContainer struct {
	Container *rabbitmq.RabbitMQContainer
	AmqpURL   string
}

// SetupRabbitMQContainer starts a RabbitMQ container for testing
func SetupRabbitMQContainer(t *testing.T) (*RabbitMQTestContainer, func()) {
	ctx := context.Background()

	// Start RabbitMQ container
	container, err := rabbitmq.Run(ctx,
		"rabbitmq:3.12-management-alpine",
		rabbitmq.WithAdminUsername("guest"),
		rabbitmq.WithAdminPassword("guest"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err, "Failed to start RabbitMQ container")

	// Get AMQP URL
	amqpURL, err := container.AmqpURL(ctx)
	require.NoError(t, err, "Failed to get AMQP URL")

	testContainer := &RabbitMQTestContainer{
		Container: container,
		AmqpURL:   amqpURL,
	}

	// Cleanup function
	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return testContainer, cleanup
}

// ConsumeMessage consumes a single message from the specified queue
func ConsumeMessage(t *testing.T, amqpURL, queueName string, timeout time.Duration) *amqp091.Delivery {
	conn, err := amqp091.Dial(amqpURL)
	require.NoError(t, err, "Failed to connect to RabbitMQ")
	defer conn.Close() //nolint:errcheck

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel")
	defer ch.Close() //nolint:errcheck

	// Declare queue (passive=false to ensure it exists)
	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // args
	)
	require.NoError(t, err, "Failed to declare queue")

	// Consume from queue
	msgs, err := ch.Consume(
		queueName,
		"",    // consumer tag
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	require.NoError(t, err, "Failed to register consumer")

	// Wait for message with timeout
	select {
	case msg := <-msgs:
		return &msg
	case <-time.After(timeout):
		t.Fatalf("Timeout waiting for message from queue %s", queueName)
		return nil
	}
}

// DecodeTaskMessage decodes the task message directly from AMQP delivery body
// The delivery body contains plain JSON TaskMessage (Celery Protocol v1 with utf-8 encoding)
func DecodeTaskMessage(t *testing.T, delivery *amqp091.Delivery) map[string]interface{} {
	require.NotNil(t, delivery, "Delivery should not be nil")

	// Verify AMQP headers and content type
	require.Equal(t, "application/json", delivery.ContentType, "Content type should be application/json")
	require.Equal(t, "utf-8", delivery.ContentEncoding, "Content encoding should be utf-8")
	require.NotNil(t, delivery.Headers, "Headers should not be nil")
	require.Equal(t, "utf-8", delivery.Headers["content-encoding"], "Body encoding should be utf-8")

	// The body is plain JSON TaskMessage (not base64-encoded)
	var taskMsg map[string]interface{}
	err := json.Unmarshal(delivery.Body, &taskMsg)
	require.NoError(t, err, "Failed to unmarshal task message")

	return taskMsg
}

// VerifyTaskMessageArgs verifies that the task message contains expected args
func VerifyTaskMessageArgs(t *testing.T, taskMsg map[string]interface{}, expectedArgs []interface{}) {
	args, ok := taskMsg["args"].([]interface{})
	require.True(t, ok, "Args should be an array")
	require.Equal(t, len(expectedArgs), len(args), "Args length mismatch")

	for i, expected := range expectedArgs {
		// Handle numeric comparison (JSON numbers are float64)
		switch v := expected.(type) {
		case int:
			require.Equal(t, float64(v), args[i], fmt.Sprintf("Args[%d] mismatch", i))
		default:
			require.Equal(t, expected, args[i], fmt.Sprintf("Args[%d] mismatch", i))
		}
	}
}

// VerifyTaskMessageKwargs verifies that the task message contains expected kwargs
// Note: This function checks that all expectedKwargs are present, but allows additional kwargs
func VerifyTaskMessageKwargs(t *testing.T, taskMsg map[string]interface{}, expectedKwargs map[string]interface{}) {
	kwargs, ok := taskMsg["kwargs"].(map[string]interface{})
	require.True(t, ok, "Kwargs should be a map")

	for key, expected := range expectedKwargs {
		actual, exists := kwargs[key]
		require.True(t, exists, fmt.Sprintf("Kwargs should contain key '%s'", key))

		// Handle numeric comparison (JSON numbers are float64)
		switch v := expected.(type) {
		case int:
			require.Equal(t, float64(v), actual, fmt.Sprintf("Kwargs['%s'] mismatch", key))
		default:
			require.Equal(t, expected, actual, fmt.Sprintf("Kwargs['%s'] mismatch", key))
		}
	}
}
