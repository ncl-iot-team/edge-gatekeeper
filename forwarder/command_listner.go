package forwarder

import (
	"fmt"
	"log"

	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

type cmdChannelConfigType struct {
	name     string
	mode     string
	protocol string
	host     string
	port     string
	user     string
	password string
	channel  string
}

// RunCommandListner listens remote commands
func RunCommandListner(cmdGoChannel chan string) {

	var cmdChannelConfig cmdChannelConfigType
	cmdChannelConfig.name = viper.GetString("forwarder.cloud-rabbitmq.name")
	cmdChannelConfig.mode = viper.GetString("forwarder.cloud-rabbitmq.mode")
	cmdChannelConfig.protocol = viper.GetString("forwarder.cloud-rabbitmq.protocol")
	cmdChannelConfig.host = viper.GetString("forwarder.cloud-rabbitmq.host")
	cmdChannelConfig.port = viper.GetString("forwarder.cloud-rabbitmq.port")
	cmdChannelConfig.user = viper.GetString("forwarder.cloud-rabbitmq.user")
	cmdChannelConfig.password = viper.GetString("forwarder.cloud-rabbitmq.password")
	cmdChannelConfig.channel = viper.GetString("forwarder.cloud-rabbitmq.channel")

	connStr := fmt.Sprintf("%s://%s:%s@%s:%s", cmdChannelConfig.protocol, cmdChannelConfig.user, cmdChannelConfig.password, cmdChannelConfig.host, cmdChannelConfig.port)

	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	/*	q, err := ch.QueueDeclare(
			"hello", // name
			false,   // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		failOnError(err, "Failed to declare a queue")
	*/
	msgs, err := ch.Consume(
		"edge-feed", // queue
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			//evalCommand(string(d.Body))
			cmdGoChannel <- string(d.Body)
		}
	}()

	log.Printf(" [*] Waiting for commands. To exit press CTRL+C")
	<-forever

}

//func evalCommand(cmd string) {
//TODO
//}
