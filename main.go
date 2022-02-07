package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mitchellh/mapstructure"
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
	pgServer = "postgres"
	pgPort   = "5432"
	pgUser   = "postgres"
	pgPW     = "adminpw"
	pgDB     = "internet_of_dinge"
	pgTable  = "iotproject.soilmoisture"

	// Insert string for postgresql database
	insert = `INSERT INTO iotproject.soilmoisture(hardware_flag,interrupt_flag,sensor_flag,tempc_ds18b20,batterie,conduct_soil,temp_soil,water_soil) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`
)

var (
	conString = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", pgServer, pgPort, pgUser, pgPW, pgDB)
)

type dbWriter struct {
	c chan map[string]interface{}
}

func NewdbWriter() *dbWriter {
	//Open connection to db here
	return &dbWriter{
		c: make(chan map[string]interface{}),
	}
}

type Message struct {
	Data map[string]interface{}
}

func (w *dbWriter) handle(_ mqtt.Client, msg mqtt.Message) {
	var m map[string]interface{}
	if err := json.Unmarshal(msg.Payload(), &m); err != nil {
		fmt.Printf("json.Unmarshall(msg.Payload(), &m):= %q, want nil", err)
	}
	for _, item := range m {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%v", item)

		if strings.Contains(buf.String(), "decoded_payload") {
			for iterator, item2 := range item.(map[string]interface{}) {
				if iterator == "decoded_payload" {
					w.c <- item2.(map[string]interface{}) // <== This will contain the magic we need.
				}
			}
		}

	}
}

func main() {

	// Setup mqtt connection
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

	// Setup postgres connection
	fmt.Println(conString)
	db, err := sql.Open("postgres", conString)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// Try to establish connection
	if err := db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	go WriteToDb(h.c, db)

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

type sensordata struct {
	Bat            float32
	Hardware_Flag  uint8
	Interrupt_Flag uint8
	Sensor_Flag    uint8
	TempC_DS18B20  uint8
	Conduct_SOIL   float32
	Temp_SOIL      string
	Water_SOIL     string
}

func WriteToDb(c chan map[string]interface{}, db *sql.DB) {
	var data sensordata
	for item := range c {
		fmt.Println(item)
		for fieldname, value := range item {
			fmt.Printf("Fieldname: %s Type: %T\n", fieldname, value)
		}
		mapstructure.Decode(item, &data)
		fmt.Println(data)
	}
}

/*
iotproject.soilmoisture
uid INTEGER NOT NULL PRIMARY KEY,
    hardware_flag VARCHAR(1),
    interrupt_flag VARCHAR(1),
    sensor_flag VARCHAR(1),
    tempc_ds18b20 REAL,
    batterie REAL,
    conduct_soil REAL,
    temp_soil REAL,
    water_soil REAL
*/
