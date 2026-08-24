package pubsub

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int
const (
	DurableQueue SimpleQueueType = iota  // 0
	TransientQueue
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	var isDurable bool
	if queueType == DurableQueue {
		isDurable = true
	} else if queueType == TransientQueue {
		isDurable = false
	} else {
		return nil, amqp.Queue{}, errors.New(
			"invalid queue type (must be either durable or transient)",
		)
	}

	t := amqp.Table{}
	t["x-dead-letter-exchange"] = "peril_dlx"

	q, err := ch.QueueDeclare(
		queueName,
		isDurable,
		!isDurable,
		!isDurable,
		false,
		t,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
    err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

    return ch, q, nil
}

