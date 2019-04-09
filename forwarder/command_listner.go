package forwarder

import (
	"fmt"
	"log"
	"strconv"
	"strings"

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

var dataRateCmdGoChanGlobal chan<- int64

var statsCmdGoChanGlobal chan<- string

// RunCommandListner listens remote commands
func RunCommandListner(dataRateCmdGoChan chan<- int64, statsCmdGoChan chan<- string) {

	dataRateCmdGoChanGlobal = dataRateCmdGoChan

	statsCmdGoChanGlobal = statsCmdGoChan

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

	go evalCommand(msgs)

	log.Printf(" [*] Waiting for commands. To exit press CTRL+C")
	<-forever

}

func evalCommand(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		log.Printf("Received a command: %s", d.Body)
		command := string(d.Body)
		if strings.HasPrefix(command, "set datarate") {

			datarateStr := strings.TrimLeft(command, "set datarate")

			//	windowsize := int64(100)

			datarate, err := strconv.ParseInt(datarateStr, 10, 32)

			failOnError(err, "Invalid parameter")

			dataRateCmdGoChanGlobal <- datarate

		} else if strings.HasPrefix(command, "get stats") {

			log.Printf("Received 'GET STATS command")

			statsCmdGoChanGlobal <- command

		} else {

			log.Printf("Invalid Command")
		}
	}
}
