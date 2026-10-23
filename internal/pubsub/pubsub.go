package pubsub

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueType int
type ExchangeType string
type AckType int

const (
	DurableQueue QueueType = iota
	TransientQueue
)

const (
	DirectExchange ExchangeType = "direct"
	TopicExchange  ExchangeType = "topic"
	FanoutExchange ExchangeType = "fanout"
)

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

// Takes a channel, an exchange name, a routing key and a payload to be converted to byte[]
// Publishes a message to queue, with the routing key using the marshaled payload as content
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	payload := amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, payload)
	if err != nil {
		return err
	}

	return nil
}

// Takes a connection, exchange and queue names, a routing key, a queueType type and a handler function
// Calls DeclareAndBind to create a queue and bind it to the given exchange
// The returned function from that handler will be executed every time a message is published to that queue
func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, exchangeType ExchangeType, queueType QueueType, handler func(T) AckType) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, exchangeType, queueType)
	if err != nil {
		return err
	}

	deliveryCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range deliveryCh {
			var data T
			json.Unmarshal(d.Body, &data)
			// handler returns an acknowledge AckType to decide if OK, requeue or discard the message
			ackValue := handler(data)
			switch ackValue {
			case Ack:
				d.Ack(false)
			case NackRequeue:
				d.Nack(false, true)
			case NackDiscard:
				d.Nack(false, false)
			}
		}
	}()

	return nil
}

// Gets a connection, exchange and queue names, a routing key and the queueType type
// Creates the queue and binds it to the exchange using the given key
// Returns the channel and the queue
func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, exchangeType ExchangeType, queueType QueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return ch, amqp.Queue{}, err
	}

	ch.ExchangeDeclare(exchange, string(exchangeType), true, false, false, false, nil)

	var quDurable bool
	var quAutoDelete bool
	var quExclusive bool

	switch queueType {
	case DurableQueue:
		quDurable = true
		quAutoDelete = false
		quExclusive = false
	case TransientQueue:
		quDurable = false
		quAutoDelete = true
		quExclusive = true
	default:
		return ch, amqp.Queue{}, errors.New("Invalid queue type")
	}

	// amqp.Table contains key-value pairs to pass arguments into QueueDeclare()
	argTable := make(map[string]any)

	argTable["x-dead-letter-exchange"] = routing.ExchangeDeadLetter

	qu, err := ch.QueueDeclare(queueName, quDurable, quAutoDelete, quExclusive, false, argTable)
	if err != nil {
		return ch, amqp.Queue{}, err
	}

	ch.QueueBind(queueName, key, exchange, false, nil)

	return ch, qu, nil
}
