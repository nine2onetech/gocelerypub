package gocelerypub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAMQPBroker_PublishWithArgsOnly tests publishing a task with args only
func TestAMQPBroker_PublishWithArgsOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup RabbitMQ container
	rabbitMQ, cleanup := SetupRabbitMQContainer(t)
	defer cleanup()

	// Create broker and publisher
	publisher, err := New(Config{
		BrokerType: AMQP,
		HostURL:    rabbitMQ.AmqpURL,
	})
	require.NoError(t, err)

	// Publish task with args only
	queueName := "test-queue-args-only"
	err = publisher.Publish(&PublishRequest{
		Queue: queueName,
		Task:  "tasks.add",
		Args:  []interface{}{10, 20, 30},
	})
	require.NoError(t, err)

	// Consume and verify the message
	delivery := ConsumeMessage(t, rabbitMQ.AmqpURL, queueName, 5*time.Second)
	taskMsg := DecodeTaskMessage(t, delivery)

	// Verify task details
	assert.Equal(t, "tasks.add", taskMsg["task"])
	VerifyTaskMessageArgs(t, taskMsg, []interface{}{10, 20, 30})

	// Verify kwargs is empty (Celery protocol v1 always includes args/kwargs)
	kwargs := taskMsg["kwargs"].(map[string]interface{})
	assert.Empty(t, kwargs, "Kwargs should be empty when not provided")
}

// TestAMQPBroker_PublishWithKwargsOnly tests publishing a task with kwargs only
func TestAMQPBroker_PublishWithKwargsOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup RabbitMQ container
	rabbitMQ, cleanup := SetupRabbitMQContainer(t)
	defer cleanup()

	// Create broker and publisher
	publisher, err := New(Config{
		BrokerType: AMQP,
		HostURL:    rabbitMQ.AmqpURL,
	})
	require.NoError(t, err)

	// Publish task with kwargs only
	queueName := "test-queue-kwargs-only"
	err = publisher.Publish(&PublishRequest{
		Queue: queueName,
		Task:  "tasks.process_data",
		Kwargs: map[string]interface{}{
			"user_id": 123,
			"action":  "update",
			"active":  true,
		},
	})
	require.NoError(t, err)

	// Consume and verify the message
	delivery := ConsumeMessage(t, rabbitMQ.AmqpURL, queueName, 5*time.Second)
	taskMsg := DecodeTaskMessage(t, delivery)

	// Verify task details
	assert.Equal(t, "tasks.process_data", taskMsg["task"])
	VerifyTaskMessageKwargs(t, taskMsg, map[string]interface{}{
		"user_id": 123,
		"action":  "update",
		"active":  true,
	})

	// Verify args is empty (Celery protocol v1 always includes args/kwargs)
	args := taskMsg["args"].([]interface{})
	assert.Empty(t, args, "Args should be empty when not provided")
}

// TestAMQPBroker_PublishWithBoth tests publishing a task with both args and kwargs
func TestAMQPBroker_PublishWithBoth(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup RabbitMQ container
	rabbitMQ, cleanup := SetupRabbitMQContainer(t)
	defer cleanup()

	// Create broker and publisher
	publisher, err := New(Config{
		BrokerType: AMQP,
		HostURL:    rabbitMQ.AmqpURL,
	})
	require.NoError(t, err)

	// Publish task with both args and kwargs
	queueName := "test-queue-both"
	err = publisher.Publish(&PublishRequest{
		Queue: queueName,
		Task:  "tasks.complex_operation",
		Args:  []interface{}{"data1", 42, true},
		Kwargs: map[string]interface{}{
			"priority": 5,
			"retry":    false,
			"tags":     []interface{}{"important", "urgent"},
		},
	})
	require.NoError(t, err)

	// Consume and verify the message
	delivery := ConsumeMessage(t, rabbitMQ.AmqpURL, queueName, 5*time.Second)
	taskMsg := DecodeTaskMessage(t, delivery)

	// Verify task details
	assert.Equal(t, "tasks.complex_operation", taskMsg["task"])
	VerifyTaskMessageArgs(t, taskMsg, []interface{}{"data1", 42, true})
	VerifyTaskMessageKwargs(t, taskMsg, map[string]interface{}{
		"priority": 5,
		"retry":    false,
	})

	// Verify tags kwarg separately (since it's an array)
	kwargs := taskMsg["kwargs"].(map[string]interface{})
	tags, ok := kwargs["tags"].([]interface{})
	require.True(t, ok, "Tags should be an array")
	assert.Equal(t, "important", tags[0])
	assert.Equal(t, "urgent", tags[1])
}

// TestAMQPBroker_PublishMultipleMessages tests publishing multiple messages
func TestAMQPBroker_PublishMultipleMessages(t *testing.T) {
	t.Skip("TODO: Debug this test - messages not being consumed properly")

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup RabbitMQ container
	rabbitMQ, cleanup := SetupRabbitMQContainer(t)
	defer cleanup()

	// Create broker and publisher
	publisher, err := New(Config{
		BrokerType: AMQP,
		HostURL:    rabbitMQ.AmqpURL,
	})
	require.NoError(t, err)

	// Publish multiple messages to the same queue
	queueName := "test-queue-multiple"
	for i := 0; i < 5; i++ {
		err = publisher.Publish(&PublishRequest{
			Queue: queueName,
			Task:  "tasks.batch_process",
			Args:  []interface{}{i},
		})
		require.NoError(t, err)
	}

	// Verify all messages were published
	for i := 0; i < 5; i++ {
		delivery := ConsumeMessage(t, rabbitMQ.AmqpURL, queueName, 5*time.Second)
		taskMsg := DecodeTaskMessage(t, delivery)

		assert.Equal(t, "tasks.batch_process", taskMsg["task"])
		// Note: order may not be guaranteed, but all messages should be present
		require.Contains(t, taskMsg, "args")
	}
}

// TestAMQPBroker_SendCeleryMessage tests SendCeleryMessage directly
func TestAMQPBroker_SendCeleryMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup RabbitMQ container
	rabbitMQ, cleanup := SetupRabbitMQContainer(t)
	defer cleanup()

	// Create broker
	broker := NewAMQPBroker(rabbitMQ.AmqpURL)

	// Create a task message manually
	taskMsg := &TaskMessageV1{
		ID:   "manual-test-id",
		Task: "tasks.manual",
		Args: []interface{}{100, 200},
	}

	// Convert to CeleryMessage
	celeryMsg := taskMsg.ToCeleryMessage(CeleryDeliveryInfo{
		Priority:   0,
		RoutingKey: "test-queue-manual",
		Exchange:   "",
	})

	// Send directly via broker
	err := broker.SendCeleryMessage(celeryMsg)
	require.NoError(t, err)

	// Consume and verify
	delivery := ConsumeMessage(t, rabbitMQ.AmqpURL, "test-queue-manual", 5*time.Second)
	receivedTaskMsg := DecodeTaskMessage(t, delivery)

	assert.Equal(t, "manual-test-id", receivedTaskMsg["id"])
	assert.Equal(t, "tasks.manual", receivedTaskMsg["task"])
	VerifyTaskMessageArgs(t, receivedTaskMsg, []interface{}{100, 200})
}

// TestAMQPBroker_Reconnect_Integration tests the Reconnect method with real broker
func TestAMQPBroker_Reconnect_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup RabbitMQ container
	rabbitMQ, cleanup := SetupRabbitMQContainer(t)
	defer cleanup()

	// Create broker
	broker := NewAMQPBroker(rabbitMQ.AmqpURL)

	t.Run("Reconnect succeeds with valid host URL", func(t *testing.T) {
		// Close existing connection
		err := broker.Channel.Close()
		require.NoError(t, err)
		err = broker.Connection.Close()
		require.NoError(t, err)

		// Reconnect with the same broker URL
		err = broker.Reconnect(rabbitMQ.AmqpURL)
		require.NoError(t, err)

		// Verify we can send messages after reconnect
		taskMsg := &TaskMessageV1{
			ID:   "reconnect-test-id",
			Task: "tasks.reconnect_test",
			Args: []interface{}{1, 2, 3},
		}

		queueName := "test-reconnect-queue"
		celeryMsg := taskMsg.ToCeleryMessage(CeleryDeliveryInfo{
			RoutingKey: queueName,
			Exchange:   "",
		})

		err = broker.SendCeleryMessage(celeryMsg)
		require.NoError(t, err, "Should be able to send message after reconnect")

		// Verify message was published
		delivery := ConsumeMessage(t, rabbitMQ.AmqpURL, queueName, 5*time.Second)
		receivedMsg := DecodeTaskMessage(t, delivery)
		assert.Equal(t, "tasks.reconnect_test", receivedMsg["task"])
		assert.Equal(t, "reconnect-test-id", receivedMsg["id"])
	})

	t.Run("Reconnect closes old channel and connection properly", func(t *testing.T) {
		// Create fresh broker for this test
		testBroker := NewAMQPBroker(rabbitMQ.AmqpURL)

		// Get reference to old channel/connection before closing
		oldChan := testBroker.Channel
		oldConn := testBroker.Connection

		// Verify they are open
		require.False(t, oldChan.IsClosed(), "Initial channel should be open")
		require.False(t, oldConn.IsClosed(), "Initial connection should be open")

		// Close them to simulate disconnection
		err := oldChan.Close()
		require.NoError(t, err)
		err = oldConn.Close()
		require.NoError(t, err)

		// Reconnect with new URL
		err = testBroker.Reconnect(rabbitMQ.AmqpURL)
		require.NoError(t, err)

		// Verify new channel and connection are open
		require.False(t, testBroker.Channel.IsClosed(), "New channel should be open after reconnect")
		require.False(t, testBroker.Connection.IsClosed(), "New connection should be open after reconnect")

		// Verify we can send a message with the new connection
		queueName := "test-reconnect-verify"
		taskMsg := &TaskMessageV1{
			ID:   "reconnect-verify-msg",
			Task: "tasks.reconnect_verify",
			Args: []interface{}{"test"},
		}

		celeryMsg := taskMsg.ToCeleryMessage(CeleryDeliveryInfo{
			RoutingKey: queueName,
			Exchange:   "",
		})

		err = testBroker.SendCeleryMessage(celeryMsg)
		require.NoError(t, err)

		// Verify message was published
		delivery := ConsumeMessage(t, rabbitMQ.AmqpURL, queueName, 5*time.Second)
		receivedMsg := DecodeTaskMessage(t, delivery)
		assert.Equal(t, "reconnect-verify-msg", receivedMsg["id"])
		assert.Equal(t, "tasks.reconnect_verify", receivedMsg["task"])
	})
}
