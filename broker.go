package go_celery_publisher

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker interface {
	CanPublish() bool
	Reconnect() error
	SendCeleryMessage(msg *CeleryMessage) error
}

type AMQPBroker struct {
	Channel    *amqp.Channel
	Connection *amqp.Connection
}

// NewAMQPConnection creates new AMQP channel
func NewAMQPConnection(host string) (*amqp.Connection, *amqp.Channel) {
	connection, err := amqp.Dial(host)
	if err != nil {
		panic(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	return connection, channel
}

// NewAMQPBroker creates new AMQPBroker
func NewAMQPBroker(host string) *AMQPBroker {
	conn, channel := NewAMQPConnection(host)
	return &AMQPBroker{
		Channel:    channel,
		Connection: conn,
	}
}

func (b *AMQPBroker) SendCeleryMessage(msg *CeleryMessage) error {
	queueName := msg.Properties.DeliveryInfo.RoutingKey
	// Declare queue
	_, err := b.Channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return err
	}

	// Use the base64-encoded body directly
	messageBody := []byte(msg.Body)

	// Convert msg.Headers to AMQP Table and add Celery-specific headers
	headers := amqp.Table{}
	if msg.Headers != nil {
		for k, v := range msg.Headers {
			headers[k] = v
		}
	}
	// Add body encoding information so Celery knows to decode from base64
	headers["content-encoding"] = msg.Properties.BodyEncoding

	// Prepare AMQP publishing
	publishing := amqp.Publishing{
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		Body:            messageBody,
		DeliveryMode:    amqp.Persistent,
		Headers:         headers,
	}

	// Publish to queue
	err = b.Channel.Publish(
		msg.Properties.DeliveryInfo.Exchange,
		queueName,
		false, // mandatory
		false, // immediate
		publishing,
	)

	return err
}

func (b *AMQPBroker) CanPublish() bool {
	return !b.isClosed()
}

func (b *AMQPBroker) isClosed() bool {
	return b.Channel.IsClosed() || b.Connection.IsClosed()
}

func (b *AMQPBroker) Reconnect() error {
	// Close existing connection and channel if open
	if !b.isClosed() {
		b.Channel.Close()
		b.Connection.Close()
	}

	// Re-establish connection and channel
	conn, channel := NewAMQPConnection(b.Connection.Config.Vhost)
	b.Connection = conn
	b.Channel = channel

	return nil
}
