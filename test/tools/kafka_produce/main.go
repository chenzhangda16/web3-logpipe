package main

import (
	"log"

	"github.com/IBM/sarama"
)

func main() {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer(
		[]string{"localhost:9092"},
		cfg,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: "raw.events",
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("hello sarama"),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("sent to partition=%d offset=%d\n", partition, offset)
}
