package forwarder

import (
	"fmt"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/viper"
)

//MQTTCommander Initiates the MQTT client and connects to the broker
func MQTTCommander(clientid string, command string) {

	topic := viper.GetString("messaging.command_topic")
	broker := viper.GetString("messaging.broker")
	//	password := viper.GetString("messaging.password")
	//	user := viper.GetString("messaging.user")
	id := clientid
	cleansess := false
	qos := 0
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

	client := MQTT.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	defer client.Disconnect(250)

	//	fmt.Printf("Edge: %s data started publishing to topic: %s \n", clientid, topic)

	//	for {
	payload := command
	//	fmt.Printf("Edge: %s going to send command %s", clientid, payload)
	token := client.Publish(topic, byte(qos), false, payload)
	token.Wait()

	//fmt.Printf("Rate of %s:%d records/sec\n", clientid, counter.Rate())
	//	}
}
