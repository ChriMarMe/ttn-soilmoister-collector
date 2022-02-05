package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	// TTN Configuration
	ttnClientID = "fhdo_docker"
	ttnServer   = "tcp://eu1.cloud.thethings.network:1883"
	ttnUsername = "soilmoisturefh@ttn"
	ttnAPIkey   = "NNSXS.RENIGXLODSFZ3CXVPJUHRV2ZQ4LABJ7JHKHU3OA.EJ2TMVG2U27UPLSL3W2CHF4ZM4G6UDH2IHHVHJLIZHCBKEHQRJLQ"
	ttnAppId    = "soilmoisturefh"
	ttnDevID    = "eui-a8404156f182ef00"
	ttnTopic    = "v3/soilmoisturefh@ttn/devices/eui-a8404156f182ef00/up"

	// DB server config
	postgreServer = "postgres:5432"
	postgresUser  = "moisterCollector"
	postgresPW    = "thisyouwantknow"

	postgresTable = "iotproject.soilmoisture"
)

type dbWriter struct{}

func NewdbWriter() *dbWriter {
	//Open connection to db here
	return &dbWriter{}
}

type Message struct {
	Data map[string]interface{}
}

func (w *dbWriter) handle(_ mqtt.Client, msg mqtt.Message) {
	var m Message
	if err := json.Unmarshal(msg.Payload(), &m.Data); err != nil {
		fmt.Printf("json.Unmarshall(msg.Payload(), &m):= %q, want nil", err)
	}
	for _, item := range m.Data {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%v", item)

		if strings.Contains(buf.String(), "decoded_payload") {
			for iterator, item2 := range item.(map[string]interface{}) {
				if iterator == "decoded_payload" {
					fmt.Printf("Item: %v\n", item2) // <== This will contain the magic we need.
				}
			}
		}

	}
}

func main() {
	mqtt.ERROR = log.New(os.Stdout, "[ERROR] ", 0)
	mqtt.CRITICAL = log.New(os.Stdout, "[CRITICAL] ", 0)
	mqtt.WARN = log.New(os.Stdout, "[WARN]  ", 0)
	mqtt.DEBUG = log.New(os.Stdout, "[DEBUG] ", 0)
	h := NewdbWriter()

	opts := mqtt.NewClientOptions().
		AddBroker(ttnServer).
		SetClientID(ttnClientID).
		SetUsername(ttnUsername).
		SetPassword(ttnAPIkey).
		SetOrderMatters(false).
		SetWriteTimeout(time.Second).
		SetKeepAlive(900 * time.Second)
	opts.ConnectRetry = true
	opts.AutoReconnect = true
	opts.PingTimeout = time.Second

	opts.DefaultPublishHandler = func(_ mqtt.Client, msg mqtt.Message) {
		fmt.Printf("UNEXPECTED MESSAGE: %s\n", msg)
	}
	opts.OnConnectionLost = func(cl mqtt.Client, err error) {
		fmt.Printf("CONNECTION LOST: %q\n", err)
	}

	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("establishing connection")

		token := c.Subscribe(ttnTopic, 0, h.handle)
		go func() {
			_ = token.Wait()
			if token.Error() != nil {
				fmt.Printf("ERROR SUBSCRIBING: %s\n", token.Error())
			} else {
				fmt.Printf("Subscribing successful to Topic %s\n", ttnTopic)
			}
		}()
	}

	opts.OnReconnecting = func(mqtt.Client, *mqtt.ClientOptions) {
		fmt.Println("attempting to reconnect")
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", opts.Servers[0])
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	fmt.Println("signal caught - exiting")
	client.Disconnect(1000)
	fmt.Println("shutdown complete")

}
