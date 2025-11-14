package gocelerypub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTaskMessageV1_Encode_WithArgsOnly tests encoding with args only
func TestTaskMessageV1_Encode_WithArgsOnly(t *testing.T) {
	taskMsg := &TaskMessageV1{
		ID:   "test-id-123",
		Task: "tasks.add",
		Args: []interface{}{1, 2, 3},
	}

	encoded, err := taskMsg.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	// Parse the JSON string (no base64 decoding needed)
	var result map[string]interface{}
	err = json.Unmarshal([]byte(encoded), &result)
	require.NoError(t, err)

	// Verify fields
	assert.Equal(t, "test-id-123", result["id"])
	assert.Equal(t, "tasks.add", result["task"])

	// Verify args
	args, ok := result["args"].([]interface{})
	require.True(t, ok, "args should be an array")
	assert.Len(t, args, 3)
	assert.Equal(t, float64(1), args[0]) // JSON numbers are float64
	assert.Equal(t, float64(2), args[1])
	assert.Equal(t, float64(3), args[2])

	// Kwargs is always present (empty map) due to Encode() initialization
	kwargs, ok := result["kwargs"].(map[string]interface{})
	require.True(t, ok, "kwargs should be present")
	assert.Empty(t, kwargs, "kwargs should be empty when not set")
}

// TestTaskMessageV1_Encode_WithKwargsOnly tests encoding with kwargs only
func TestTaskMessageV1_Encode_WithKwargsOnly(t *testing.T) {
	taskMsg := &TaskMessageV1{
		ID:   "test-id-456",
		Task: "tasks.process",
		Kwargs: map[string]interface{}{
			"name":  "test",
			"count": 42,
			"flag":  true,
		},
	}

	encoded, err := taskMsg.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	// Parse the JSON string (no base64 decoding needed)
	var result map[string]interface{}
	err = json.Unmarshal([]byte(encoded), &result)
	require.NoError(t, err)

	// Verify fields
	assert.Equal(t, "test-id-456", result["id"])
	assert.Equal(t, "tasks.process", result["task"])

	// Verify kwargs
	kwargs, ok := result["kwargs"].(map[string]interface{})
	require.True(t, ok, "kwargs should be a map")
	assert.Equal(t, "test", kwargs["name"])
	assert.Equal(t, float64(42), kwargs["count"]) // JSON numbers are float64
	assert.Equal(t, true, kwargs["flag"])

	// Args is always present (empty array) due to Encode() initialization
	args, ok := result["args"].([]interface{})
	require.True(t, ok, "args should be present")
	assert.Empty(t, args, "args should be empty when not set")
}

// TestTaskMessageV1_Encode_WithBoth tests encoding with both args and kwargs
func TestTaskMessageV1_Encode_WithBoth(t *testing.T) {
	taskMsg := &TaskMessageV1{
		ID:   "test-id-789",
		Task: "tasks.complex",
		Args: []interface{}{"arg1", 123, true},
		Kwargs: map[string]interface{}{
			"key1": "value1",
			"key2": 456,
		},
	}

	encoded, err := taskMsg.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	// Parse the JSON string (no base64 decoding needed)
	var result map[string]interface{}
	err = json.Unmarshal([]byte(encoded), &result)
	require.NoError(t, err)

	// Verify fields
	assert.Equal(t, "test-id-789", result["id"])
	assert.Equal(t, "tasks.complex", result["task"])

	// Verify args
	args, ok := result["args"].([]interface{})
	require.True(t, ok, "args should be an array")
	assert.Len(t, args, 3)
	assert.Equal(t, "arg1", args[0])
	assert.Equal(t, float64(123), args[1])
	assert.Equal(t, true, args[2])

	// Verify kwargs
	kwargs, ok := result["kwargs"].(map[string]interface{})
	require.True(t, ok, "kwargs should be a map")
	assert.Equal(t, "value1", kwargs["key1"])
	assert.Equal(t, float64(456), kwargs["key2"])
}

// TestTaskMessageV1_ToCeleryMessage tests CeleryMessage conversion
func TestTaskMessageV1_ToCeleryMessage(t *testing.T) {
	taskMsg := &TaskMessageV1{
		ID:   "test-id-celery",
		Task: "tasks.test",
		Args: []interface{}{1, 2},
		Kwargs: map[string]interface{}{
			"key": "value",
		},
	}

	deliveryInfo := CeleryDeliveryInfo{
		Priority:   0,
		RoutingKey: "test-queue",
		Exchange:   "",
	}

	celeryMsg := taskMsg.ToCeleryMessage(deliveryInfo)

	// Verify CeleryMessage structure
	assert.NotNil(t, celeryMsg)
	assert.Equal(t, "application/json", celeryMsg.ContentType)
	assert.Equal(t, "utf-8", celeryMsg.ContentEncoding)
	assert.NotEmpty(t, celeryMsg.Body)

	// Verify body is plain JSON (not base64 encoded)
	var taskData map[string]interface{}
	err := json.Unmarshal([]byte(celeryMsg.Body), &taskData)
	require.NoError(t, err)
	assert.Equal(t, "test-id-celery", taskData["id"])
	assert.Equal(t, "tasks.test", taskData["task"])

	// Verify properties
	assert.Equal(t, "utf-8", celeryMsg.Properties.BodyEncoding)
	assert.Equal(t, 2, celeryMsg.Properties.DeliveryMode)
	assert.NotEmpty(t, celeryMsg.Properties.CorrelationID)
	assert.NotEmpty(t, celeryMsg.Properties.ReplyTo)
	assert.NotEmpty(t, celeryMsg.Properties.DeliveryTag)

	// Verify delivery info
	assert.Equal(t, 0, celeryMsg.Properties.DeliveryInfo.Priority)
	assert.Equal(t, "test-queue", celeryMsg.Properties.DeliveryInfo.RoutingKey)
	assert.Equal(t, "", celeryMsg.Properties.DeliveryInfo.Exchange)
}

// TestTaskMessageV1_GetID tests GetID method
func TestTaskMessageV1_GetID(t *testing.T) {
	taskMsg := &TaskMessageV1{
		ID:   "unique-task-id",
		Task: "tasks.example",
	}

	assert.Equal(t, "unique-task-id", taskMsg.GetID())
}

// TestTaskMessageV1_Encode_EmptyArgs tests that nil args are omitted due to omitempty
func TestTaskMessageV1_Encode_EmptyArgs(t *testing.T) {
	taskMsg := &TaskMessageV1{
		ID:   "test-empty",
		Task: "tasks.empty",
		Args: nil, // Explicitly nil
	}

	encoded, err := taskMsg.Encode()
	require.NoError(t, err)

	// Parse the JSON string (no base64 decoding needed)
	var result map[string]interface{}
	err = json.Unmarshal([]byte(encoded), &result)
	require.NoError(t, err)

	// Args is always present as empty array (Encode() initializes nil args)
	args, ok := result["args"].([]interface{})
	require.True(t, ok, "args should be present")
	assert.Empty(t, args, "nil args should be initialized to empty array by Encode()")
}
