package forwarder

import (
	"fmt"
	"os"

	MQTT "github.com/eclipse/paho.mqtt.golang"

	//	PSUTILCPU "github.com/shirou/gopsutil/cpu"
	//	PSUTILMEM "github.com/shirou/gopsutil/mem"
	"github.com/spf13/viper"
)

//InitMQTTStatsClient Initiates the MQTT client and connects to the broker
func InitMQTTStatsClient(clientid string, mqttStoredMessages *string) {

	topic := "$SYS/broker/store/messages/count"
	broker := viper.GetString("messaging.broker")
	id := clientid + "_stats"
	cleansess := true
	qos := 0
	store := ":memory:"

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

	for {
		incoming := <-choke
		if incoming[0] == topic {
			*mqttStoredMessages = incoming[1]
		}
	}

}
