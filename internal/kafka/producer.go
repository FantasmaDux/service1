package kafka

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type Producer struct {
	producer *kafka.Producer
}
