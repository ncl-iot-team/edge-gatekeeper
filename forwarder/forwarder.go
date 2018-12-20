package forwarder

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
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
	channel    string
	windowsize string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// RunForwarder runs all the configured forwarders
func RunForwarder(windowSizeCmdGoChan <-chan int64) {

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
	fwrdrConfig.channel = viper.GetString("forwarder.cloud-rabbitmq.channel")
	windowsize, err := strconv.ParseInt(fwrdrConfig.windowsize, 10, 64)
	//ratecounterwindowsec, err := strconv.ParseInt(viper.GetString("forwarder.cloud-rabbitmq.ratecounterwindowsec"), 10, 64)

	go func() {
		for msg := range windowSizeCmdGoChan {
			windowsize = msg
		}
	}()

	failOnError(err, "Invalid Config")

	connStr := fmt.Sprintf("%s://%s:%s@%s:%s", fwrdrConfig.protocol, fwrdrConfig.user, fwrdrConfig.password, fwrdrConfig.host, fwrdrConfig.port)
	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		fwrdrConfig.channel, // name
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare a queue")

	fileNameStr := fmt.Sprintf("%s/%s", viper.GetString("listeners.tsensor1.path"), viper.GetString("listeners.tsensor1.filename"))

	failOnError(err, "Failed to open the file")

	doCount := make(chan bool)
	go NewRateCounter(time.Second*10, doCount)

	log.Printf("Reading file and sending data to queue '%s'", q.Name)
	// Infinite Loop to send records foreever (For testing and benchmarking)

	for {
		file, err := os.Open(fileNameStr)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		//	defer

		reader := bufio.NewReader(file)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {

			line := scanner.Text()
			body := line
			doACalc()
			err = ch.Publish("", // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
				})
			doCount <- true
			//	log.Printf("Sent: %s", body)
			failOnError(err, "Failed to publish a message")
			time.Sleep(time.Millisecond * time.Duration(windowsize))
		}
		file.Close()
	}

}

// Dummy calculation to pe
func doACalc() {
	done := make(chan int)

	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}

	time.Sleep(time.Millisecond * 1)
	close(done)
}
