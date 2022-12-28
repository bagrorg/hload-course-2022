package server_backend

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	urlTopicName = "dkulikov-urls"
	delim        = '|'
)

var cmap kafka.ConfigMap = kafka.ConfigMap{"bootstrap.servers": "158.160.19.212:9092"}
var kafkaProducer *kafka.Producer = func() *kafka.Producer {
	p, err := kafka.NewProducer(&cmap)
	if err != nil {
		panic(fmt.Sprintf("Kafka error: %s", err))
	}
	return p
}()

func packUrlMsg(tinyUrl string, longUrl string) []byte {
	return []byte(tinyUrl + string(delim) + longUrl)
}

func unpackUrlMsg(b []byte) (string, string) {
	p := string(b)
	d := 0
	for i := 0; i < len(p); i++ {
		if p[i] == delim {
			d = i
		}
	}

	return p[0:d], p[d:]
}

func PushCreatedUrl(tinyUrl string, longUrl string) {
	// Delivery report handler for produced messages
	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := urlTopicName
	kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          packUrlMsg(tinyUrl, longUrl),
	}, nil)

	// Wait for message deliveries before shutting down
	kafkaProducer.Flush(15 * 1000)
}

// Read kafka url topic
