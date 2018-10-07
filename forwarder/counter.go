package forwarder

import (
	"encoding/json"
	"fmt"
	"time"

	"bitbucket.org/bertimus9/systemstat"
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
	stats := newStats()
	stats.GatherStats(true)
	//stats.PrintStats()
	stats.setStats()
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
	simpleStats := getStats()
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

//StatsChannelConfig structure keeps the channel configuration
type StatsChannelConfig struct {
	name     string
	mode     string
	protocol string
	host     string
	port     string
	user     string
	password string
	queue    string
}

//Stats struct stores the stats
type Stats struct {
	startTime time.Time

	// stats this process
	ProcUptime        float64 //seconds
	ProcMemUsedPct    float64
	ProcCPUAvg        systemstat.ProcCPUAverage
	LastProcCPUSample systemstat.ProcCPUSample `json:"-"`
	CurProcCPUSample  systemstat.ProcCPUSample `json:"-"`

	// stats for whole system
	LastCPUSample systemstat.CPUSample `json:"-"`
	CurCPUSample  systemstat.CPUSample `json:"-"`
	SysCPUAvg     systemstat.CPUAverage
	SysMemK       systemstat.MemSample
	LoadAverage   systemstat.LoadAvgSample
	SysUptime     systemstat.UptimeSample

	// bookkeeping
	procCPUSampled bool
	sysCPUSampled  bool
}

// SimpleStatsData stores basic stats infomation (CPU and memory)
type SimpleStatsData struct {
	IdleCPU float32
	IdleMen float32
}

var simpleStats SimpleStatsData

//NewStats creates new stats
func newStats() *Stats {
	s := Stats{}
	s.startTime = time.Now()
	return &s
}

func (s *Stats) setStats() {
	fmt.Printf("Cpu(s): %.1f%%id\n", s.SysCPUAvg.IdlePct)
	fmt.Printf("Mem: %9dk free\n", s.SysMemK.MemFree)
	simpleStats = SimpleStatsData{float32(s.SysCPUAvg.IdlePct), float32(s.SysMemK.MemFree)}
}

//GatherStats gathers stats
func (s *Stats) GatherStats(percent bool) {
	s.SysUptime = systemstat.GetUptime()
	s.ProcUptime = time.Since(s.startTime).Seconds()

	s.SysMemK = systemstat.GetMemSample()
	s.LoadAverage = systemstat.GetLoadAvgSample()

	s.LastCPUSample = s.CurCPUSample
	s.CurCPUSample = systemstat.GetCPUSample()

	if s.sysCPUSampled { // we need 2 samples to get an average
		s.SysCPUAvg = systemstat.GetCPUAverage(s.LastCPUSample, s.CurCPUSample)
	}
	// we have at least one sample, subsequent rounds will give us an average
	s.sysCPUSampled = true

	s.ProcMemUsedPct = 100 * float64(s.CurProcCPUSample.ProcMemUsedK) / float64(s.SysMemK.MemTotal)

	s.LastProcCPUSample = s.CurProcCPUSample
	s.CurProcCPUSample = systemstat.GetProcCPUSample()
	if s.procCPUSampled {
		s.ProcCPUAvg = systemstat.GetProcCPUAverage(s.LastProcCPUSample, s.CurProcCPUSample, s.ProcUptime)
	}
	s.procCPUSampled = true
}

/*
// RunMonitoringD runs the monitoring daemon and continously retrives and calculate system stats.
func RunMonitoringD(statsCmdGoChan <-chan string) {

	fmt.Println("Starting System Stat Monitoring")

	var statschanconfig StatsChannelConfig

	statschanconfig.name = viper.GetString("monitoring-manager.main.name")
	statschanconfig.mode = viper.GetString("monitoring-manager.main.mode")
	statschanconfig.protocol = viper.GetString("monitoring-manager.main.protocol")
	statschanconfig.host = viper.GetString("monitoring-manager.main.host")
	statschanconfig.port = viper.GetString("monitoring-manager.main.port")
	statschanconfig.user = viper.GetString("monitoring-manager.main.user")
	statschanconfig.password = viper.GetString("monitoring-manager.main.password")
	statschanconfig.queue = viper.GetString("monitoring-manager.main.queue")

	log.Printf("Running Monitoring Agent Daemon..")

	stats := NewStats()

	connStr := fmt.Sprintf("%s://%s:%s@%s:%s", statschanconfig.protocol, statschanconfig.user, statschanconfig.password, statschanconfig.host, statschanconfig.port)
	log.Printf("Connection String: %s", connStr)
	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		statschanconfig.queue, // name
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// This next line lets out see the jsonified object
	// produced by systemstat
	//	printJson(stats, false)

	for msg := range statsCmdGoChan {

		stats.GatherStats(true)
		//stats.PrintStats()
		stats.SetStats()

		statMode := strings.TrimLeft(msg, "get stats")

		switch statMode {
		case "full":
			log.Printf("Sending full stats to ")

		default:
			log.Printf("Sending Basic Stats to the server..")
			simpleStats := GetStats()
			b := &simpleStats
			bodyjson, err := json.Marshal(b)
			if err != nil {
				fmt.Println(err)
				return
			}
			body := string(bodyjson)
			err = ch.Publish("", // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
				})

			log.Printf("Sent: %s", body)
			failOnError(err, "Failed to publish a message")
			log.Printf("published %dB OK", len(body))
		}
		//failOnError(err, "Invalid Command")
	}

}
*/
//GetStats returns the simple stats
func getStats() SimpleStatsData {
	return simpleStats
}
