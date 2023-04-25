package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/kafka-lib/mq"
)

const topic = "confluent_test3"

func main() {
	if err := kafka.InitV2(mq.Addresses("10.0.0.161:9092")); err != nil {
		logrus.Fatal(err)
	}

	if err := kafka.Connect(); err != nil {
		logrus.Fatal(err)
	}

	i := 0
	for {
		err := kafka.Publish(topic, &mq.Message{
			Body: []byte(strconv.Itoa(i)),
		})
		if err != nil {
			logrus.Fatal(err)
		}

		//err = kafka.Publish("confluent_test", &mq.Message{
		//	Body: []byte(strconv.Itoa(i)),
		//})
		//if err != nil {
		//	logrus.Fatal(err)
		//}

		fmt.Printf("send ok %d \n", i)

		i++

		time.Sleep(time.Second * 1)
	}
}
