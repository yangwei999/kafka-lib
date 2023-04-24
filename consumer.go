package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/kafka-lib/mq"
)

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

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

	select {
	case sig := <-sigchan:
		fmt.Printf("Caught signal %v: terminating\n", sig)
		s.Unsubscribe()
	}

}
