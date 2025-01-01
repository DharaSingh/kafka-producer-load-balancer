package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// Consumer is the struct that implements the sarama.ConsumerGroupHandler interface
type Consumer struct {
	speed int // local variable to sleep for these many seconds, to mimic slow and fast consumer behaviour
}

// Setup is called once at the start of the consumer group
func (Consumer) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer setup")
	return nil
}

// Cleanup is called once at the end of the consumer group
func (Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer cleanup")
	return nil
}

// ConsumeClaim is called when messages are fetched for a specific partition
func (cc Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Iterate through the messages in the claim (messages are partitioned)
	for message := range claim.Messages() {
		time.Sleep(time.Second * time.Duration(cc.speed))
		// Process the message
		fmt.Printf("Consumed message by consumer-speed = %d, Key = %s, Value = %s, Partition = %d, Offset = %d\n", cc.speed,
			string(message.Key), string(message.Value), message.Partition, message.Offset)

		// Mark the message as processed
		session.MarkMessage(message, "")
	}
	return nil
}

// RunConsumer - Basic consumer implementation from GPT
func RunConsumer(brokers []string, topic, cg, clientId string, speed int) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.ClientID = clientId
	// Create a new Kafka consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokers, cg, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close() // Ensure the consumer group is properly closed

	// Initialize the consumer
	consumer := Consumer{speed: speed}

	// Create a context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())

	// Set up signal handling for graceful shutdown (e.g., SIGINT or SIGTERM)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalChan)

	// Start consuming messages in a separate goroutine
	go func() {
		for {
			// The first argument is the context (which can be canceled)
			// The second argument is the list of topics to consume from
			// The third argument is the handler that processes the consumed messages
			err := consumerGroup.Consume(ctx, []string{topic}, &consumer)
			if err != nil {
				log.Errorf("Error consuming messages: %v", err)
				return
			}
		}
	}()

	// Wait for a termination signal (e.g., SIGINT or SIGTERM)
	<-signalChan
	fmt.Println("Shutting down consumer gracefully...")
	cancel() // Cancel the context to stop consuming

	// Optionally, you can wait for the goroutine to finish, if needed
	time.Sleep(1 * time.Second)
}
