package forwarder

import (
	"context"
	"fmt"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"golang.org/x/time/rate"
)

type forwarderConfigStruct struct {
	name     string
	mode     string
	protocol string
	host     string
	port     string
	user     string
	password string
	channel  string
	datarate int64
}

//RunForwarderWithRateLimiter runs the MQTT Client and Forwards the data to AMQP broker in cloud using.
//This also applies rate limiter
func RunForwarderWithRateLimiter(dataRateCmdGoChan <-chan (int64)) {

	fmt.Println("Starting Forwarder")

	var fwrdrConfig forwarderConfigStruct

	fwrdrConfig.name = viper.GetString("forwarder.cloud-rabbitmq.name")
	fwrdrConfig.mode = viper.GetString("forwarder.cloud-rabbitmq.mode")
	fwrdrConfig.protocol = viper.GetString("forwarder.cloud-rabbitmq.protocol")
	fwrdrConfig.host = viper.GetString("forwarder.cloud-rabbitmq.host")
	fwrdrConfig.port = viper.GetString("forwarder.cloud-rabbitmq.port")
	fwrdrConfig.user = viper.GetString("forwarder.cloud-rabbitmq.user")
	fwrdrConfig.password = viper.GetString("forwarder.cloud-rabbitmq.password")
	fwrdrConfig.datarate = viper.GetInt64("forwarder.cloud-rabbitmq.datarate")
	fwrdrConfig.channel = viper.GetString("forwarder.cloud-rabbitmq.channel")

	clientid := viper.GetString("device.id")
	deliverybuffer := viper.GetInt64("forwarder.deliverybuffer")

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

	deliveries := make(chan string, deliverybuffer)

	dataratereadseconds := viper.GetInt("messaging.dataratereadseconds")

	// Rate limiter limits the number of sensor values read per second
	limit := rate.Limit(fwrdrConfig.datarate)
	limiter := rate.NewLimiter(limit, 1)
	cntx := context.Background()

	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("Config file changed2:", e.Name, e.Op.String())
		newlimit := viper.GetInt64("forwarder.cloud-rabbitmq.datarate")
		limit := rate.Limit(fwrdrConfig.datarate)
		limiter.SetLimit(limit)
		fmt.Printf("New datarate set for %s: %d records/sec\n", clientid, newlimit)

	})

	//Go routine to set new limit
	go func() {
		newlimit := fwrdrConfig.datarate
		for {
			newlimit = <-dataRateCmdGoChan
			limit := rate.Limit(newlimit)
			limiter.SetLimit(limit)
			fmt.Printf("New datarate set for %s: %d records/sec\n", clientid, newlimit)
		}
	}()

	// Go routine to initiate and run the MQTT Client
	go InitMQTTClient(clientid, &deliveries, dataratereadseconds)

	for {

		//fmt.Println("InLoop")
		msg := <-deliveries

		//fmt.Println("Publishing messsage: " + msg)

		limiter.Wait(cntx)
		err = ch.Publish("", // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg),
			})

	}

}
