package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)


type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType,  // an enum to represent "durable" or "transient"
	handler func(T) AckType,
	t amqp.Table,
) error {
	ch, _, err := DeclareAndBind(
		conn,
		exchange, queueName, key,
		queueType,
		t,
	)	
	if err != nil {
		return err
	}

	deliveriesCh, err := ch.Consume(queueName, "", false, false, false, false, nil)  // empty consumer string => auto-generated
	if err != nil {
		return err
	}
    
	go func() {
		var goerr error
		for delivery := range deliveriesCh {
			var tmp T
			goerr = json.Unmarshal(delivery.Body, &tmp)
			if goerr != nil {
				log.Printf("go routine error in `json.Unmarshal`: %v", goerr)
				continue
			}

			ak := handler(tmp)
			switch ak {
			case Ack:
				goerr = delivery.Ack(false)
				if goerr != nil {
					log.Printf("go routine error in `delivery.Ack`: %v", goerr)
				}
			case NackRequeue:
				goerr = delivery.Nack(false, true)
				if goerr != nil {
					log.Printf("go routine error in `delivery.Nack(false, true)`: %v", goerr)
				}
			case NackDiscard:
				goerr = delivery.Nack(false, false)
				if goerr != nil {
					log.Printf("go routine error in `delivery.Nack(false, false)`: %v", goerr)
				}
			}

		}
	}()


	return nil
}








