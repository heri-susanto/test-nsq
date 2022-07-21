package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/nsqio/go-nsq"
)

var consumerChannel chan Log
var done chan struct{}

type Log struct {
	IPAddress  string    `json:"ip_address"`
	Payload    string    `json:"payload"`
	StatusCode int       `json:"status_code"`
	CreatedAt  time.Time `json:"created_at"`
}

type messageHandler struct{}

// HandleMessage implements the Handler interface.
func (h *messageHandler) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		// In this case, a message with an empty body is simply ignored/discarded.
		return nil
	}
	otpLog := Log{}
	err := json.Unmarshal(m.Body, &otpLog)
	if err != nil {
		return err
	}
	consumerChannel <- otpLog
	return nil
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
	if len(data) > 0 {
		query, params := prepareQueryLog(data)
		_, err := conn.Exec(query, params...)
		if err != nil {
			// need to requeue the message, for now just print error
			fmt.Println(err.Error())
		}
		// fmt.Println("Inserted: ", len(data))
	}
}

func consumeMessage(wg *sync.WaitGroup) {
	connectionString :=
		fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", "postgres", "", "postgres")

	conn, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	interval := time.NewTicker(time.Second * 10) // ticker of every 10 second
	threshold := 1
	bulkArray := make([]Log, 0)

	for {
		select {
		case <-interval.C:
			AddMongoBulk(conn, bulkArray)
			bulkArray = nil

		case msg := <-consumerChannel:
			bulkArray = append(bulkArray, msg)
			if len(bulkArray) >= threshold { // pushing 1 messages to postgres at a time
				AddMongoBulk(conn, bulkArray)
				bulkArray = nil
			}
		case <-done:
			log.Println("Goroutine Closed")
			AddMongoBulk(conn, bulkArray)
			wg.Done()

		}
	}
}

func main() {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	done = make(chan struct{})
	consumerChannel = make(chan Log, 1) // channel of buffer 1, if buffer = 0, it will be a blocking channel

	go consumeMessage(wg)

	config := nsq.NewConfig()
	nsqTopic := "otp_log_topic_test"
	nsqChannel := "otp_log_channel_test"
	consumer, err := nsq.NewConsumer(nsqTopic, nsqChannel, config)
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddHandler(&messageHandler{})
	err = consumer.ConnectToNSQLookupd("localhost:4161")
	if err != nil {
		log.Fatal(err)
	}
	start_time := time.Now()
	log.Println(start_time.Local().String() + "Awaiting messages from NSQ topic: " + nsqTopic)

	// wait for signal to exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	done <- struct{}{}

	// Gracefully stop the consumer.
	consumer.Stop()
	log.Println("Done")
	wg.Wait()
}
