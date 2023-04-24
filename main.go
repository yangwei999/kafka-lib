package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/kafka-lib/mq"
)

func main() {
	if err := kafka.InitV2(mq.Addresses("10.0.0.161:9092")); err != nil {
		logrus.Fatal(err)
	}

	fmt.Println("1")

	if err := kafka.Connect(); err != nil {
		logrus.Fatal(err)
	}

	fmt.Println("2")

	s, err := kafka.Subscribe("confulent_wawa", "dada", func(event mq.Event) error {
		fmt.Println(string(event.Message().Body))

		return nil
	})
	if err != nil {
		logrus.Fatal(err)
	}

	fmt.Println("3")

	i := 0
	for {
		err = kafka.Publish("confulent_wawa", &mq.Message{
			Body: []byte(strconv.Itoa(i)),
		})
		if err != nil {
			logrus.Fatal(err)
		}

		fmt.Println("send ok")
		i++

		if i == 50 {
			if err = s.Unsubscribe(); err != nil {
				fmt.Println(err)
				return
			}
		}

		time.Sleep(time.Second * 1)
	}

}
