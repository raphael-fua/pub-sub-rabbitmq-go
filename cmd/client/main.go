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

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not welcome client: %v", err)
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect, routing.PauseKey + "." + username, routing.PauseKey,
		pubsub.TransientQueue,
	)
	if err != nil {
		log.Fatalf("could not create channel from connection: %v", err)
	}

	gamestate := gamelogic.NewGameState(username)
	gameLoop:
		for {
			words := gamelogic.GetInput()
			if len(words) == 0 {
				continue
			} 
			switch words[0] {
			case "spawn":
				log.Print("`spawn` client command")
				err = gamestate.CommandSpawn(words)
                if err != nil {
					fmt.Printf("`gamestate.CommandSpawn` error: %v", err)
				}
			case "move":
				log.Print("`move` client command")
				_, err := gamestate.CommandMove(words)
				if err != nil {
					fmt.Printf("`gamestate.CommandMove` error: %v", err)
				}
			case "status":
				log.Print("`status` client command")
				gamestate.CommandStatus()
			case "help":
				log.Print("`help` client command")
				gamelogic.PrintClientHelp()
			case "spam":
				log.Print("`spam` client command")  // not yet allowed
			case "quit":
				log.Print("`quit` client command")
				gamelogic.PrintQuit()
				break gameLoop
			default:
				log.Print("unknown client command")
			}
		}


	// wait for ctrl + c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nRabbitMQ connection closed.")
}



