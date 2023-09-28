package utils

import (
	"context"
	"log"

	"github.com/k0msak007/go-kafka/config"
	"github.com/segmentio/kafka-go"
)

func KafkaConn(cfg *config.KafkaConfig) *kafka.Conn {
	conn, err := kafka.DialLeader(context.Background(), "tcp", cfg.Url, cfg.Topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	return conn
}

func IsTopicAlreadyExists(conn *kafka.Conn, topic string) bool {
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		panic(err)
	}
	for _, p := range partitions {
		if p.Topic == topic {
			return true
		}
	}
	return false
}
