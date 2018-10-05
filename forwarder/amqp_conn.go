package forwarder

import (
	"fmt"
	"log"

	"github.com/nipunbalan/edge-mon/amqp"
)

//AMQPConnDetailsType is the structure definition for holding connection information
type AMQPConnDetailsType struct {
	user     string
	password string
	host     string
	port     string
	queue    string
}

//NewConsumer creates a consumer and returns the delivery channel
func NewConsumer(amqpConnDetails AMQPConnDetailsType, deliveries chan string) {

	connStr := fmt.Sprintf("amqp://%s:%s@%s:%s", amqpConnDetails.user, amqpConnDetails.password, amqpConnDetails.host, amqpConnDetails.port)
	log.Printf("Connection String: %s", connStr)
	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		amqpConnDetails.queue, // queue
		"",                    // consumer
		true,                  // auto-ack
		false,                 // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	for d := range msgs {
		deliveries <- string(d.Body)
	}

	//	deliveries <- msgs

	log.Printf(" [*] Waiting for commands. To exit press CTRL+C")
	<-forever

}

//NewProducer creates a producer
func NewProducer(amqpConnDetails AMQPConnDetailsType, deliveries *chan string) {

	connStr := fmt.Sprintf("amqp://%s:%s@%s:%s", amqpConnDetails.user, amqpConnDetails.password, amqpConnDetails.host, amqpConnDetails.port)
	log.Printf("Connection String: %s", connStr)
	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		amqpConnDetails.queue, // name
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for {

		body := <-*deliveries
		err = ch.Publish("", // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})

		failOnError(err, "Failed to publish a message")
	}

}
