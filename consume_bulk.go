package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	nsq "github.com/nsqio/go-nsq"
)

var consumerChannel chan Log

type Log struct {
	IPAddress  string    `json:"ip_address"`
	Payload    string    `json:"payload"`
	StatusCode int       `json:"status_code"`
	CreatedAt  time.Time `json:"created_at"`
}

func prepareQueryLog(data []Log) (string, []interface{}) {
	query := "INSERT INTO otp_log(ip_address, payload, status_code, created_at) VALUES "
	params := []interface{}{}
	x := 1

	for _, logData := range data {
		query += fmt.Sprintf("($%d, $%d, $%d, $%d),", x, x+1, x+2, x+3)
		params = append(params, logData.IPAddress, logData.Payload, logData.StatusCode, logData.CreatedAt)
		x += 4
	}

	query = strings.TrimSuffix(query, ",")
	query += " ON CONFLICT ON CONSTRAINT unix_otp_log DO NOTHING"
	return query, params
}

func AddMongoBulk(conn *sql.DB, data []Log) {
	query, params := prepareQueryLog(data)
	_, err := conn.Exec(query, params...)
	if err != nil {
		fmt.Println(err.Error(), "INI ADALAH ERROR")
	}
}

var handleMessage = func(msg *nsq.Message) error {
	otpLog := Log{}
	_ = json.Unmarshal(msg.Body, &otpLog)

	consumerChannel <- otpLog
	return nil
}

func consumeMessage() {
	connectionString :=
		fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", "postgres", "", "postgres")

	conn, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	interval := time.NewTicker(time.Second * 10) // ticker of every 10 second
	threshold := 1000
	bulkArray := make([]Log, 0)

	for {
		select {
		case <-interval.C:
			AddMongoBulk(conn, bulkArray)
			bulkArray = nil

		case msg := <-consumerChannel:
			bulkArray = append(bulkArray, msg)
			if len(bulkArray) >= threshold { // pushing 1000 messages to postgres at a time
				AddMongoBulk(conn, bulkArray)
				bulkArray = nil
			}
		}
	}
}

func InitNsqConsumer() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	consumerChannel = make(chan Log, 1) // channel of buffer 1, if buffer = 0, it will be a blocking channel
	go consumeMessage()

	nsqTopic := "otp_log_topic"
	nsqChannel := "otp_log_channel"
	consumer, err := nsq.NewConsumer(nsqTopic, nsqChannel, nsq.NewConfig())

	if err != nil {
		log.Print("error create consumer")
	}
	consumer.AddHandler(nsq.HandlerFunc(handleMessage))
	err = consumer.ConnectToNSQLookupd("127.0.0.1:4161")
	if err != nil {
		log.Print("error connecting to nsqlookupd")
	} else {
		log.Print("BERHASIL KONEK")
	}
	log.Println("Awaiting messages from NSQ topic \"otp_log_topic\"...")
	wg.Wait()
}

func main() {
	InitNsqConsumer()
}