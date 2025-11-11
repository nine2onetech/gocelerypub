package go_celery_publisher

import (
	"encoding/json"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// CeleryMessage is actual message to be sent to the broker
// https://docs.celeryq.dev/projects/kombu/en/stable/_modules/kombu/message.html
type CeleryMessage struct {
	Body            string                 `json:"body"`
	Headers         map[string]interface{} `json:"headers,omitempty"`
	ContentType     string                 `json:"content-type"`
	Properties      CeleryProperties       `json:"properties"`
	ContentEncoding string                 `json:"content-encoding"`
}

// CeleryProperties represents properties json
type CeleryProperties struct {
	BodyEncoding  string             `json:"body_encoding"`
	CorrelationID string             `json:"correlation_id"`
	ReplyTo       string             `json:"reply_to"`
	DeliveryInfo  CeleryDeliveryInfo `json:"delivery_info"`
	DeliveryMode  int                `json:"delivery_mode"`
	DeliveryTag   string             `json:"delivery_tag"`
}

// CeleryDeliveryInfo represents deliveryinfo json
type CeleryDeliveryInfo struct {
	Priority   int    `json:"priority"`
	RoutingKey string `json:"routing_key"`
	Exchange   string `json:"exchange"`
}

// TaskMessage is interface for celery task messages
// TaskMessage composes the body of CeleryMessage
type TaskMessage interface {
	ToCeleryMessage(deliveryInfo CeleryDeliveryInfo) *CeleryMessage
	Encode() (string, error)
	GetID() string
}

// TaskMessageV1 is celery-compatible message (protocol v1)
// https://celery-safwan.readthedocs.io/en/latest/internals/protocol.html#version-1
type TaskMessageV1 struct {
	ID        string                 `json:"id"`
	Task      string                 `json:"task"`
	Args      []interface{}          `json:"args"`
	Kwargs    map[string]interface{} `json:"kwargs"`
	Retries   int                    `json:"retries,omitempty"`
	ETA       *string                `json:"eta,omitempty"`
	Expires   *string                `json:"expires,omitempty"`
	Taskset   string                 `json:"taskset,omitempty"`   // Group ID (also called group)
	UTC       bool                   `json:"utc,omitempty"`       // Whether to use UTC timezone
	TimeLimit []float64              `json:"timelimit,omitempty"` // [soft, hard] time limits in seconds
}

// Encode returns json encoded string (without base64)
func (tm *TaskMessageV1) Encode() (string, error) {
	if tm.Args == nil {
		tm.Args = make([]interface{}, 0)
	}
	if tm.Kwargs == nil {
		tm.Kwargs = make(map[string]interface{})
	}
	jsonData, err := json.Marshal(tm)
	if err != nil {
		return "", err
	}
	return string(jsonData), err
}

func (tm *TaskMessageV1) ToCeleryMessage(deliveryInfo CeleryDeliveryInfo) *CeleryMessage {
	encodedBody, _ := tm.Encode()
	return &CeleryMessage{
		Body: encodedBody,
		Headers: map[string]interface{}{
			"lang":    "go",
			"task":    tm.Task,
			"id":      tm.ID,
			"retries": 0,
		},
		ContentType: "application/json",
		Properties: CeleryProperties{
			BodyEncoding:  "utf-8",
			CorrelationID: uuid.New().String(),
			ReplyTo:       uuid.New().String(),
			DeliveryInfo:  deliveryInfo,
			DeliveryMode:  int(amqp.Persistent),
			DeliveryTag:   uuid.New().String(),
		},
		ContentEncoding: "utf-8",
	}
}

func (tm *TaskMessageV1) GetID() string {
	return tm.ID
}
