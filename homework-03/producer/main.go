package main

import (
	"database/sql"
	"fmt"
	"main/server_backend"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/lib/pq"
)

const SQL_DRIVER = "postgres"

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "jaja"
	dbname   = "hload"
)

func cumsume() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "158.160.19.212:9092",
		"group.id":          "myGroup2",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"dkulikov-urls"}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

func main() {
	go func() {
		cumsume()
	}()

	fmt.Println(sql.Drivers())
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	conn, err := sql.Open(SQL_DRIVER, psqlInfo)
	if err != nil {
		fmt.Println("Failed to open", err)
		panic("exit")
	}

	err = conn.Ping()
	if err != nil {
		fmt.Println("Failed to ping database", err)
		panic("exit")
	}

	_, err = conn.Exec("create table if not exists urls(id bigint, url varchar unique)")
	if err != nil {
		fmt.Println("Failed to create table", err)
		panic("exit")
	}

	pr := server_backend.SetupPrometheusRouter()
	r := server_backend.SetupRouter(conn)
	go pr.Run(":2112")
	err = r.Run(":8080")
	if err != nil {
		panic("Something wrong with router: " + err.Error())
	}
}
