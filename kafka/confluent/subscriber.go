package confluent

import (
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/mq"
)

func newSubscriber(broker, topic, group string, handler mq.Handler) (sub *subscriber, err error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        broker,
		"group.id":                 group,
		"auto.offset.reset":        "earliest",
		"allow.auto.create.topics": true,
		"enable.auto.commit":       false,
	})
	if err != nil {
		return
	}

	if err = consumer.Subscribe(topic, nil); err != nil {
		return
	}

	sub = &subscriber{
		consumer:   consumer,
		handler:    handler,
		commitChan: make(chan *kafka.Message, 100),
		stopRead:   make(chan struct{}),
	}

	return
}

type subscriber struct {
	consumer *kafka.Consumer
	handler  mq.Handler

	commitChan chan *kafka.Message
	stopRead   chan struct{}

	wg sync.WaitGroup
}

func (s *subscriber) start() {
	go s.process()

	s.wg.Add(1)
	go s.commit()
}

func (s *subscriber) process() {
	for {
		select {
		case <-s.stopRead:
			close(s.commitChan)

			return
		default:
			msg, err := s.consumer.ReadMessage(time.Second)
			if err == nil {
				e := newEvent(msg)
				if err = s.handler(e); err != nil {
					logrus.Errorf("handle msg error: %s", err.Error())
				}

				// commit offset async
				s.commitChan <- msg
			} else if !err.(kafka.Error).IsTimeout() {
				logrus.Errorf("consumer error: %v (%v)", err, msg)
			}
		}
	}
}

func (s *subscriber) commit() {
	for m := range s.commitChan {
		if _, err := s.consumer.CommitMessage(m); err != nil {
			logrus.Errorf("commit error: %v (%v)", err, m)
		}
	}

	s.wg.Done()
}

func (s *subscriber) Options() mq.SubscribeOptions {
	return mq.SubscribeOptions{}
}

func (s *subscriber) Topic() string {
	topics, _ := s.consumer.Subscription()

	return strings.Join(topics, ",")
}

func (s *subscriber) Unsubscribe() error {
	close(s.stopRead)

	s.wg.Wait()

	return s.consumer.Close()
}
