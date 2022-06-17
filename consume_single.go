package main

// import (
// 	"database/sql"
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"sync"
// 	"time"

// 	_ "github.com/lib/pq"

// 	"github.com/nsqio/go-nsq"
// )

// type Log struct {
// 	IPAddress  string    `json:"ip_address"`
// 	Payload    string    `json:"payload"`
// 	StatusCode int       `json:"status_code"`
// 	CreatedAt  time.Time `json:"created_at"`
// }

// func main() {
// 	wg := &sync.WaitGroup{}
// 	wg.Add(1)

// 	connectionString :=
// 		fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", "postgres", "", "postgres")

// 	conn, err := sql.Open("postgres", connectionString)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	decodeConfig := nsq.NewConfig()
// 	c, err := nsq.NewConsumer("otp_log_topic", "otp_log_channel", decodeConfig)
// 	if err != nil {
// 		log.Panic("Could not create consumer")
// 	}
// 	//c.MaxInFlight defaults to 1

// 	c.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
// 		log.Println("NSQ message received:")

// 		// save ke postgres untuk monitoring

// 		otpLog := Log{}
// 		json.Unmarshal(message.Body, &otpLog)

// 		log.Println(otpLog, "INI OTP LOG YES")
// 		log.Println(otpLog.CreatedAt, "INI OTP LOG YES")

// 		err := conn.QueryRow(
// 			"INSERT INTO otp_log(ip_address, status_code, payload, created_at) VALUES ($1, $2, $3, $4)",
// 			otpLog.IPAddress, otpLog.StatusCode, otpLog.Payload, otpLog.CreatedAt)
// 		if err != nil {
// 			fmt.Println(err.Err(), "ini error")
// 		} else {
// 			fmt.Println("MASHOK YESS!")
// 		}

// 		return nil
// 	}))

// 	err = c.ConnectToNSQD("127.0.0.1:4150")
// 	if err != nil {
// 		log.Panic("Could not connect")
// 	}
// 	log.Println("Awaiting messages from NSQ topic \"otp_log_topic\"...")
// 	wg.Wait()
// }
