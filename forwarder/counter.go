package forwarder

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

//var count int64

// NewRateCounter initiates a new counter
func NewRateCounter(window time.Duration, countChannel chan bool) {

	var amqpConnDetails AMQPConnDetailsType

	amqpConnDetails.host = viper.GetString("remote-log-collector.main.host")
	amqpConnDetails.port = viper.GetString("remote-log-collector.main.port")
	amqpConnDetails.user = viper.GetString("remote-log-collector.main.user")
	amqpConnDetails.password = viper.GetString("remote-log-collector.main.password")
	amqpConnDetails.queue = viper.GetString("remote-log-collector.main.queue")

	var count int64

	deliveries := make(chan string, 4096)

	go NewProducer(amqpConnDetails, &deliveries)

	// Function generates ticks at the end of the configured window
	go func() {
		for {
			time.Sleep(window)
			recordCount(window, &count, &deliveries)
		}
	}()

	for range countChannel {
		count++
	}

}

func recordCount(window time.Duration, count *int64, deliveries *chan string) {
	//	log.Printf("Count: %d", *count)
	*deliveries <- fmt.Sprintf("%s | E1 Sent Count: %d", time.Now().Format(time.RFC3339), *count)
	*count = 0

}
