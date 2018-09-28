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
	cmdChannelConfig.name = viper.GetString("remote-commander.main.name")
	cmdChannelConfig.mode = viper.GetString("remote-commander.main.mode")
	cmdChannelConfig.protocol = viper.GetString("remote-commander.main.protocol")
	cmdChannelConfig.host = viper.GetString("remote-commander.main.host")
	cmdChannelConfig.port = viper.GetString("remote-commander.main.port")
	cmdChannelConfig.user = viper.GetString("remote-commander.main.user")
	cmdChannelConfig.password = viper.GetString("remote-commander.main.password")
	cmdChannelConfig.channel = viper.GetString("remote-commander.main.channel")

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
		cmdChannelConfig.channel, // queue
		"",                       // consumer
		true,                     // auto-ack
		false,                    // exclusive
		false,                    // no-local
		false,                    // no-wait
		nil,                      // args
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
