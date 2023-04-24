package main

import (
	"fmt"

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

	s, err := kafka.Subscribe(topic, "dada", func(event mq.Event) error {
		fmt.Println(string(event.Message().Body))

		return nil
	})
	if err != nil {
		logrus.Fatal(err)
	}

	defer s.Unsubscribe()
}
