package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func HandlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func HandlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		mo := gs.HandleMove(mv)
		switch mo {
		case gamelogic.MoveOutComeSafe:
			log.Println("Move ack")
			return pubsub.Ack

		case gamelogic.MoveOutcomeMakeWar:
			log.Println("Move ack")
			return pubsub.Ack

		case gamelogic.MoveOutcomeSamePlayer:
			log.Println("Move nack discard")
			return pubsub.NackDiscard

		default:
			log.Println("Move nack discard")
			return pubsub.NackDiscard
		}

	}
}
