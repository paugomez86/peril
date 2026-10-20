package pubsub

import (
	"context"
	"encoding/json"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueType int

const (
	Durable = iota
	Transient
)

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

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType QueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return ch, amqp.Queue{}, err
	}

	var quDurable bool
	var quAutoDelete bool
	var quExclusive bool

	switch queueType {
	case Durable:
		quDurable = true
		quAutoDelete = false
		quExclusive = false
	case Transient:
		quDurable = false
		quAutoDelete = true
		quExclusive = true
	default:
		return ch, amqp.Queue{}, errors.New("Invalid queue type")
	}

	qu, err := ch.QueueDeclare(queueName, quDurable, quAutoDelete, quExclusive, false, nil)
	if err != nil {
		return ch, amqp.Queue{}, err
	}

	ch.QueueBind(queueName, key, exchange, false, nil)

	return ch, qu, nil
}
