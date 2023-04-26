package confluent

import (
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/mq"
)

func newSubscriber(broker, group string) (*subscriber, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          group,
		// action to take when there is no initial offset in offset store or
		// the desired offset is out of range
		"auto.offset.reset": "earliest",
		// the broker must also be configured with auto.create.topics.enable=true
		// for this configuration to take effect
		"allow.auto.create.topics": true,
		// automatic commit can result in repeated consumption and message loss
		"enable.auto.commit": false,
	})

	if err != nil {
		return nil, err
	}

	return &subscriber{
		consumer:   consumer,
		handlers:   make(Handlers),
		commitChan: make(chan *kafka.Message, 100),
		stopRead:   make(chan struct{}),
	}, nil
}

type subscriber struct {
	consumer *kafka.Consumer
	handlers Handlers

	commitChan chan *kafka.Message
	stopRead   chan struct{}

	wg sync.WaitGroup
}

func (s *subscriber) subscribe(h Handlers) error {
	s.handlers = h

	var topics []string
	for t, _ := range s.handlers {
		topics = append(topics, t)
	}

	return s.consumer.SubscribeTopics(topics, nil)
}

func (s *subscriber) start() {
	// handle message in a goroutine
	go s.process()

	s.wg.Add(1)
	// commit message in another goroutine
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

				handler, ok := s.handlers[*msg.TopicPartition.Topic]
				if !ok {
					continue
				}

				if err = handler(e); err != nil {
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
	if s.consumer.IsClosed() {
		return nil
	}

	close(s.stopRead)

	s.wg.Wait()

	return s.consumer.Close()
}
