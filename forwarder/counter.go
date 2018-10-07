package forwarder

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nipunbalan/edge-gatekeeper/monitoring"
	"github.com/spf13/viper"
)

//var count int64

// NewRateCounter initiates a new counter
func NewRateCounter(window time.Duration, countChannel chan bool) {

	var rateDataForwarderAMQPConnDetails AMQPConnDetailsType

	rateDataForwarderAMQPConnDetails.host = viper.GetString("remote-log-collector.main.host")
	rateDataForwarderAMQPConnDetails.port = viper.GetString("remote-log-collector.main.port")
	rateDataForwarderAMQPConnDetails.user = viper.GetString("remote-log-collector.main.user")
	rateDataForwarderAMQPConnDetails.password = viper.GetString("remote-log-collector.main.password")
	rateDataForwarderAMQPConnDetails.queue = viper.GetString("remote-log-collector.main.queue")
	deviceid := viper.GetString("device.id")

	var count int64

	deliveries := make(chan string, 4096)

	go NewProducer(rateDataForwarderAMQPConnDetails, &deliveries)

	// Create new stats
	stats := monitoring.NewStats()
	stats.GatherStats(true)
	//stats.PrintStats()
	stats.SetStats()
	// Function generates a record at the end of the configured window
	go func() {
		for {
			time.Sleep(window)
			recordCount(deviceid, window, &count, &deliveries)
		}
	}()

	for range countChannel {
		count++
	}

}

func recordCount(deviceid string, window time.Duration, count *int64, deliveries *chan string) {
	//	log.Printf("Count: %d", *count)
	simpleStats := monitoring.GetStats()
	b := &simpleStats
	bodyjson, err := json.Marshal(b)
	if err != nil {
		fmt.Println(err)
		return
	}

	*deliveries <- fmt.Sprintf("%s | %s Sent Count: %d  |%s", time.Now().Format(time.RFC3339), deviceid, *count, bodyjson)
	*count = 0

}

//NewSystemStatsForwarder forwards systems stats to a queue
/*func NewSystemStatsForwarder(window time.Duration, countChannel chan bool) {

	var sysStatsForwarderAMQPConnDetails AMQPConnDetailsType

	sysStatsForwarderAMQPConnDetails.host = viper.GetString("monitoring-manager.main.host")
	sysStatsForwarderAMQPConnDetails.port = viper.GetString("monitoring-manager.main.port")
	sysStatsForwarderAMQPConnDetails.user = viper.GetString("monitoring-manager.main.user")
	sysStatsForwarderAMQPConnDetails.password = viper.GetString("monitoring-manager.main.password")
	sysStatsForwarderAMQPConnDetails.queue = viper.GetString("monitoring-manager.main.queue")

	deviceid := viper.GetString("device.id")

	statsDeliveries := make(chan string, 4096)

	go NewProducer(sysStatsForwarderAMQPConnDetails, &statsDeliveries)

}
*/
