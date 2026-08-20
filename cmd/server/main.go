package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// connectionString := "amqp://guest:guest@127.0.0.1:5672/"
	connectionString := "amqp://guest:guest@localhost:5672/"
	connectionPointer, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer connectionPointer.Close()

	fmt.Println("Starting Peril server...")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	
	fmt.Println("\nClosing Peril server...")
}



