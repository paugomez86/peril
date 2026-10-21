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

	connCh, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	defer connCh.Close()

	fmt.Println("Connection to RabbitMQ server successful")

	// Queue/exchange bindings
	logsCh, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	defer logsCh.Close()

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
			pubsub.PublishJSON(connCh, routing.ExchangePerilDirect, routing.PauseKey, payload)

		case "resume":
			fmt.Println("Sending resume")
			payload := routing.PlayingState{
				IsPaused: false,
			}
			pubsub.PublishJSON(connCh, routing.ExchangePerilDirect, routing.PauseKey, payload)

		case "quit":
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
