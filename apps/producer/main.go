package main

import (
	"log"
	"time"

	"github.com/k0msak007/go-kafka/config"
	"github.com/k0msak007/go-kafka/models"
	"github.com/k0msak007/go-kafka/pkg/utils"
	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := &config.KafkaConfig{
		Url:   "localhost:9092",
		Topic: "shop",
	}

	conn := utils.KafkaConn(cfg)

	if !utils.IsTopicAlreadyExists(conn, cfg.Topic) {
		topicConfigs := []kafka.TopicConfig{
			{
				Topic:             cfg.Topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		}

		err := conn.CreateTopics(topicConfigs...)
		if err != nil {
			panic(err)
		}
	}

	data := func() []kafka.Message {
		products := []models.Product{
			{
				Id:    "2dc7cf08-e238-4faa-bd5f-f1cfe2e0b565",
				Title: "Coffee",
			},
			{
				Id:    "4c56ec5b-d638-42f2-ae1d-38b6fc6d2122",
				Title: "Tea",
			},
			{
				Id:    "36da5a84-f333-4ecf-a2fe-130c3e8d4ef1",
				Title: "Milk",
			},
		}

		message := make([]kafka.Message, 0)
		for _, p := range products {
			message = append(message, kafka.Message{
				Value: utils.CompressToJson(&p),
			})
		}

		return message
	}()

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err := conn.WriteMessages(data...)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
