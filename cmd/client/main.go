package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("error in `conn.Channel`: %v", err)
	}
	defer publishCh.Close()
	fmt.Println("Peril client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not welcome client: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	t := amqp.Table{}
	t["x-dead-letter-exchange"] = "peril_dlx"
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gs),
		t,
	)
	if err != nil {
		log.Fatalf("error in `pubsub.SubscribeJSON`: %v", err)
	}
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*",
		pubsub.TransientQueue,
		handlerMove(gs, publishCh),
		t,
	)
	if err != nil {
		log.Fatalf("error in `pubsub.SubscribeJSON`: %v", err)
	}

	// warRecognitionCh, q, err := pubsub.DeclareAndBind(
	// _, q, err := pubsub.DeclareAndBind(
	// 	conn,
	// 	routing.ExchangePerilTopic,
	// 	routing.WarRecognitionsPrefix,
	// 	routing.WarRecognitionsPrefix + ".*",
	// 	pubsub.DurableQueue,
	// )
	// if err != nil {
	// 	log.Fatalf("error in `pubsub.DeclareAndBind`: %v", err)
	// }
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.DurableQueue,
		handlerConsumeAllWarMessages(gs),
		nil,
	)
	if err != nil {
		log.Fatalf("error in pubsub.SubscribeJSON: %v", err)
	}
	// warRecognitionCh.Consume(q.Name, username, false, false, false, false, nil)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			log.Print("`spawn` client command")
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Printf("`gs.CommandSpawn` error: %v", err)
			}
		case "move":
			log.Print("`move` client command")
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("`gs.CommandMove` error: %v", err)
				continue
			}
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username,
				mv,
			)
			if err != nil {
				log.Printf("error in `pubsub.PublishJSON`: %v", err)
				continue
			}
			log.Print("`move` published successfully")
		case "status":
			log.Print("`status` client command")
			gs.CommandStatus()
		case "help":
			log.Print("`help` client command")
			gamelogic.PrintClientHelp()
		case "spam":
			log.Print("`spam` client command") // not yet allowed
		case "quit":
			log.Print("`quit` client command")
			// gamelogic.PrintQuit()
			// signalChan := make(chan os.Signal, 1)
			// signal.Notify(signalChan, os.Interrupt)
			// <-signalChan
			// fmt.Println("\nRabbitMQ connection closed.")
			return
		default:
			log.Print("unknown client command")
		}
	}
}

