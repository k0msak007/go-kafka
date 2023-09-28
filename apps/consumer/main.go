package main

import (
	"fmt"
	"log"

	"github.com/k0msak007/go-kafka/config"
	"github.com/k0msak007/go-kafka/pkg/utils"
)

func main() {
	cfg := &config.KafkaConfig{
		Url:   "localhost:9092",
		Topic: "shop",
	}

	conn := utils.KafkaConn(cfg)

	for {
		message, err := conn.ReadMessage(10e3)
		if err != nil {
			break
		}

		fmt.Println(string(message.Value))
	}

	if err := conn.Close(); err != nil {
		log.Fatalf("failed to close connection: %v", err)
	}
}
