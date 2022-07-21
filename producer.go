package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
)

func main() {
	config := nsq.NewConfig()
	p, err := nsq.NewProducer("127.0.0.1:4150", config)
	if err != nil {
		log.Panic(err)
	}
	for x := 0; x < 1000000; x++ {
		nsqPayload := map[string]interface{}{"payload": string("ini payload"), "ip_address": "1.1.1.1", "status_code": 200, "created_at": time.Now()}

		nsqBody, _ := json.Marshal(nsqPayload)
		err = p.Publish("otp_log_topic_test", []byte(nsqBody))
		if err != nil {
			log.Panic(err)
		}
	}

}


