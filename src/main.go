// Purpose: Publish and instantly consume a message sent to RabbitMq queue.

// Requires RabbitMq 3.x and Golang 1.7.x to be installed.
// go get -u "github.com/streadway/amqp"
// go run main.go

package main

import (
	"fmt"
	"log"

	"time"

	"github.com/streadway/amqp"
)

func main() {
	fmt.Println("Sending messages")
	go server()
	go client()

	var a string
	fmt.Scanln(&a)
}
func client() {
	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	failOnError(err, "Failed to setup Consumer")

	for msg := range msgs {
		fmt.Printf("Message Received: %s\n", msg.Body)
	}
}
func server() {
	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()

	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("What's up. Am I in the queue?"),
	}
	for range time.Tick(time.Second * 1) {
		err := ch.Publish("", q.Name, false, false, msg)
		failOnError(err, "Unable to Publish")
	}

}

func getQueue() (*amqp.Connection, *amqp.Channel, *amqp.Queue) {
	conn, err := amqp.Dial("amqp://guest@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to connect to RabbitMQ Channel")

	q, err := ch.QueueDeclare("pluralsight.go_dist", false, false, false, false, nil)
	failOnError(err, "Failed to connect to RabbitMQ Queue")

	return conn, ch, &q
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s : %s\n", msg, err)
		panic(fmt.Sprintf("%s : %s\n", msg, err))
	}
}
