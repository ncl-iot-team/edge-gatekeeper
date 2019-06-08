package forwarder

import (
	"fmt"
	"os"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	RATECOUNTER "github.com/paulbellamy/ratecounter"
	PSUTILCPU "github.com/shirou/gopsutil/cpu"
	PSUTILMEM "github.com/shirou/gopsutil/mem"
)

//InitMQTTClient Initiates the MQTT client and connects to the broker
func InitMQTTClient(clientid string, deliveries *chan string, dataRateDisplayInterval int, topic string, broker string) {

	//topic := viper.GetString("messaging.data_topic")
	//broker := viper.GetString("messaging.broker")
	//	password := viper.GetString("messaging.password")
	//	user := viper.GetString("messaging.user")
	id := clientid
	cleansess := false
	qos := 1
	store := ":memory:"

	//fmt.Println("Topic:" + broker)

	//topic = strings.Replace(topic, "{sensor_name}", clientid, -1)

	if topic == "" {
		fmt.Println("Invalid topic, must not be empty")
		return
	}

	if broker == "" {
		fmt.Println("Invalid broker URL, must not be empty")
		return
	}

	opts := MQTT.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(id)
	//	opts.SetUsername(user)
	//	opts.SetPassword(password)
	opts.SetCleanSession(cleansess)
	if store != ":memory:" {
		opts.SetStore(MQTT.NewFileStore(store))
	}

	choke := make(chan [2]string)

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		choke <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	client := MQTT.NewClient(opts)
	defer client.Disconnect(250)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe(topic, byte(qos), nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	counter := RATECOUNTER.NewRateCounter(1 * time.Second)

	//Go routine to print out data sending rate

	if dataRateDisplayInterval != 0 {
		go func() {
			for {
				percent, _ := PSUTILCPU.Percent(0, true)
				mem, _ := PSUTILMEM.VirtualMemory()

				//fmt.Printf("%s | Data receive rate at '%s' : %d \t records/sec\n", time.Now().Format(time.RFC3339), clientid, counter.Rate())
				//	fmt.Printf("%d | Data receive rate at '%s' : %d \t records/sec\n | CPU:%d", time.Now().UnixNano(), clientid, counter.Rate(), percent[0])
				//fmt.Printf("%d | Data receive rate at '%s' : %d \t records/sec\n ", time.Now().UnixNano(), clientid, counter.Rate())
				fmt.Printf("%d,%d,%f,%f,%f,%f,%f\n", time.Now().UnixNano(), counter.Rate(), percent[0], percent[1], percent[2], percent[3], mem.UsedPercent)
				//fmt.Printf("%d,%d\n", time.Now().UnixNano(), counter.Rate())
				time.Sleep(time.Second * time.Duration(dataRateDisplayInterval))
			}
		}()
	}
	//i := 1
	for {
		incoming := <-choke
		//	fmt.Printf("Message No: %d\n", i)
		//	i++
		*deliveries <- incoming[1]
		counter.Incr(1)
		//	fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
	}

}
