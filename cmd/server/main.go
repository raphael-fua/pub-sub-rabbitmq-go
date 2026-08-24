package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	// var game_logs pubsub.SimpleQueueType = pubsub.DurableQueue

	

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")
	gamelogic.PrintServerHelp()

	t := amqp.Table{}
	t["x-dead-letter-exchange"] = "peril_dlx"
	ch, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		"game_logs.*",
		pubsub.DurableQueue,
		t,
	)
	if err != nil {
		log.Fatalf("could not create channel from connection: %v", err)
	}

	gameLoop:
		for {
			words := gamelogic.GetInput()
			if len(words) == 0 {
				continue
			} 
			switch words[0] {

			case "help":
				log.Print("`help` server command")
				log.Println("Possible commands:")
				log.Println("  * help")
				log.Println("  * pause")
				log.Println("  * quit")
				log.Println("  * resume")
			case "pause":
				log.Print("`pause` server command")
				err = pubsub.PublishJSON(
					ch,
					routing.ExchangePerilDirect,
					routing.PauseKey,
					routing.PlayingState{
						IsPaused: true,
					},
				)
				if err != nil {
					log.Printf("error is `pubsub.PublishJSON`: %v", err)
				}
			case "resume":
				log.Print("`resume` server command")
				err = pubsub.PublishJSON(
					ch,
					routing.ExchangePerilDirect,
					routing.PauseKey,
					routing.PlayingState{
						IsPaused: false,
					},
				)
				if err != nil {
					log.Printf("error is `pubsub.PublishJSON`: %v", err)
				}
			case "quit":
				log.Print("`quit` server command")
				break gameLoop
			default:
				log.Print("unknown server command")
			}
		}

	// wait for ctrl + c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nRabbitMQ connection closed.")
}



