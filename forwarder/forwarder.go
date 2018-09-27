package forwarder

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

type forwarderConfig struct {
	name       string
	mode       string
	protocol   string
	host       string
	port       string
	user       string
	password   string
	windowsize string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// RunForwarder runs all the configured forwarders
func RunForwarder(cmdGoChannel chan string) {

	fmt.Println("Starting Forwarder")

	var fwrdrConfig forwarderConfig

	fwrdrConfig.name = viper.GetString("forwarder.cloud-rabbitmq.name")
	fwrdrConfig.mode = viper.GetString("forwarder.cloud-rabbitmq.mode")
	fwrdrConfig.protocol = viper.GetString("forwarder.cloud-rabbitmq.protocol")
	fwrdrConfig.host = viper.GetString("forwarder.cloud-rabbitmq.host")
	fwrdrConfig.port = viper.GetString("forwarder.cloud-rabbitmq.port")
	fwrdrConfig.user = viper.GetString("forwarder.cloud-rabbitmq.user")
	fwrdrConfig.password = viper.GetString("forwarder.cloud-rabbitmq.password")
	fwrdrConfig.windowsize = viper.GetString("forwarder.cloud-rabbitmq.windowsize")
	windowsize, err := strconv.ParseInt("100", 10, 32)

	failOnError(err, "Invalid Config")

	connStr := fmt.Sprintf("%s://%s:%s@%s:%s", fwrdrConfig.protocol, fwrdrConfig.user, fwrdrConfig.password, fwrdrConfig.host, fwrdrConfig.port)
	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"edge-feed", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for i := 0; i < 10000000; i++ {
		body := fmt.Sprintf("sensor_id:aq_mesh1758150, metric: Temperature, unit:Celsius, timestamp: %v, value: %v", time.Now().UnixNano(), float32(17+rand.Intn(5))+rand.Float32())
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		log.Printf(" [x] Sent %s", body)
		failOnError(err, "Failed to publish a message")
		time.Sleep(time.Millisecond * time.Duration(windowsize))
	}

}
