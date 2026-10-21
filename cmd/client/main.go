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
	fmt.Println("Connection to RabbitMQ server successful")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	gs := gamelogic.NewGameState(username)

repl:
	for {
		input := gamelogic.GetInput()
		switch input[0] {
		case "spawn":
			err := gs.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
			}

		case "move":
			_, err := gs.CommandMove(input)
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
