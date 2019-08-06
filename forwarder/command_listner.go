package forwarder

import (
	"fmt"
	"log"
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

var sensorCommandChannel chan<- string

var statsCmdGoChanGlobal chan<- string

// RunCommandListner listens remote commands
func RunCommandListner(dataRateCmdGoChan chan<- int64, sensorCommandChannel chan<- string, statsCmdGoChan chan<- string) {

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

		command := strings.ToLower(string(d.Body))

		if strings.HasPrefix(command, "set e_fwd_rt") {

			//	windowsize := int64(100)
			log.Printf("Received 'SET E_FWD_RT command :%s", command)

			thisDevice := viper.GetString("device.id")

			deviceID := thisDevice

			datarate := viper.GetInt64("forwarder.cloud-rabbitmq.datarate")

			//		datarate, err := strconv.ParseInt(datarateStr, 10, 32)

			_, err := fmt.Sscanf(command, "set e_fwd_rt %s %d", &deviceID, &datarate)

			if deviceID == deviceID {

				dataRateCmdGoChanGlobal <- datarate

			}

			failOnError(err, "Invalid command")

		} else if strings.HasPrefix(command, "set s_rt") {

			log.Printf("Received 'SET S_RT command :%s", command)

			MQTTCommander(viper.GetString("device.id"), command)

		} else if strings.HasPrefix(command, "get stats") {

			log.Printf("Received 'GET STATS command")

			statsCmdGoChanGlobal <- command

		} else {

			log.Printf("Invalid Command")
		}
	}
}
