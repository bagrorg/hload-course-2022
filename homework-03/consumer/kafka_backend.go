package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	urlTopicName = "dkulikov-urls"
	delim        = '|'
)

var cmap kafka.ConfigMap = kafka.ConfigMap{
	"bootstrap.servers":  "***",
	"group.id":           "amogus",
	"auto.offset.reset":  "earliest",
	"enable.auto.commit": "false",
}

var kafkaConsumer *kafka.Consumer = func() *kafka.Consumer {
	c, err := kafka.NewConsumer(&cmap)
	if err != nil {
		panic(fmt.Sprintf("Kafka error: %s\n", err))
	}

	c.SubscribeTopics([]string{urlTopicName}, nil)

	return c
}()

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

func KafkaDvij() {
	for {
		msg, err := kafkaConsumer.ReadMessage(time.Second)
		if err != nil {
			fmt.Printf("Kafka error: %s\n", err)
			continue
		}

		err = SetLongUrl(unpackUrlMsg(msg.Value))
		if err != nil {
			fmt.Printf("Something went wrong with Redis: %s", err)
		}

		kafkaConsumer.Commit() // TODO: not sure
	}
}
