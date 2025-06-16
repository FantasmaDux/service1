package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/lib/pq"
)

const (
	dbHost     = "postgreDB_service1"
	dbPort     = 5432
	dbUser     = "postgres"
	dbPassword = "postgres"
	dbName     = "testdb"

	kafkaBroker = "kafka1:29092,kafka2:29093,kafka3:29094"
)

var kafkaTopic = "test-topic"

func main() {
	// --- PostgreSQL section ---
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Database unreachable:", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	_, err = db.Exec(`INSERT INTO users (name) VALUES ($1)`, "Alice")
	if err != nil {
		log.Fatal("Failed to insert user:", err)
	}

	rows, err := db.Query(`SELECT id, name FROM users`)
	if err != nil {
		log.Fatal("Failed to query users:", err)
	}
	defer rows.Close()

	fmt.Println("Users:")
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		fmt.Printf("ID: %d, Name: %s\n", id, name)
	}

	// --- Kafka section ---
	go startConsumer()

	// Подождём немного, чтобы consumer успел запуститься
	time.Sleep(2 * time.Second)

	err = produceMessage("Hello from Confluent Kafka Go!")
	if err != nil {
		log.Fatal("Kafka produce error:", err)
	}

	// Подождём, пока consumer получит сообщение
	time.Sleep(5 * time.Second)
}

func produceMessage(message string) error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"debug":             "broker",
	})
	if err != nil {
		return err
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, deliveryChan)

	if err != nil {
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}

	fmt.Printf("Delivered message to %v\n", m.TopicPartition)
	close(deliveryChan)
	return nil
}

func startConsumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
		"debug":             "consumer,cgrp",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{kafkaTopic}, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Kafka consumer started. Waiting for messages...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		msg, err := c.ReadMessage(10 * time.Second) // +++ Используем ReadMessage вместо Poll
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue
			}
			log.Printf("Consumer error: %v\n", err)
			continue
		}

		log.Printf("Received message: %s (partition %d at offset %d)\n",
			string(msg.Value),
			msg.TopicPartition.Partition,
			msg.TopicPartition.Offset)
	}
}
