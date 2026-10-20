package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connString := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	fmt.Println("Connection to RabbitMQ server successful")

	gamelogic.PrintServerHelp()

repl:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("Sending pause")
			payload := routing.PlayingState{
				IsPaused: true,
			}
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, payload)

		case "resume":
			fmt.Println("Sending resume")
			payload := routing.PlayingState{
				IsPaused: false,
			}
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, payload)

		case "quit":
			fmt.Println("Quitting")
			break repl

		default:
			fmt.Println("Invalid command")
		}
	}

	/* signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan */

	fmt.Println("\nShutting down...")
	conn.Close()
}
