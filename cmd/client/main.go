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
	fmt.Println("Starting Peril client...")

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
	fmt.Println("Connection to RabbitMQ server successful")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("pause.%s", username), routing.PauseKey, pubsub.Transient, HandlerPause(gs))
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), "army_moves.*", pubsub.Transient, HandlerMove(gs))
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

repl:
	for {
		input := gamelogic.GetInput()
		switch input[0] {
		// Calling gamelogic spawn function
		case "spawn":
			err := gs.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
			}

		case "move":
			// Calling gamelogic move function
			mv, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println(err)
			}
			// Publishing message with army_moves.username key
			err = pubsub.PublishJSON(connCh, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), mv)
			if err != nil {
				fmt.Println(err)
			}

		case "status":
			gs.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			break repl
		}
	}
	fmt.Println("\nShutting down...")
	conn.Close()
}
