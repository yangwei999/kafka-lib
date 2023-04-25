package confluent

import (
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/mq"
)

func NewConfluentMQ() mq.MQ {
	return &Confluent{}
}

type Confluent struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
	opts     mq.Options
	broker   string

	commitChan chan *kafka.Message
	stopRead   chan struct{}
	wg         sync.WaitGroup
}

func (c *Confluent) Init(opts ...mq.Option) error {
	for _, o := range opts {
		o(&c.opts)
	}

	if c.opts.Addresses == nil {
		c.opts.Addresses = []string{"127.0.0.1:9092"}
	}

	c.broker = strings.Join(c.opts.Addresses, ",")

	c.commitChan = make(chan *kafka.Message, 10000)

	c.stopRead = make(chan struct{})

	return nil
}

func (c *Confluent) Options() mq.Options {
	return c.opts
}

func (c *Confluent) Address() string {
	if len(c.opts.Addresses) > 0 {
		return c.opts.Addresses[0]
	}

	return ""
}

func (c *Confluent) Connect() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": c.broker})
	if err != nil {
		return err
	}

	c.producer = p

	return nil
}

func (c *Confluent) Disconnect() error {
	c.producer.Close()

	return nil
}

func (c *Confluent) Publish(topic string, msg *mq.Message, opts ...mq.PublishOption) error {
	return c.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          msg.Body,
		Key:            []byte(msg.MessageKey()),
		Timestamp:      time.Now(),
	}, nil)
}

func (c *Confluent) Subscribe(topic, group string, h mq.Handler) (s mq.Subscriber, err error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        c.broker,
		"group.id":                 group,
		"auto.offset.reset":        "earliest",
		"allow.auto.create.topics": true,
		"enable.auto.commit":       false,
	})
	if err != nil {
		return
	}

	c.consumer = consumer

	if err = c.consumer.Subscribe(topic, nil); err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-c.stopRead:
				close(c.commitChan)

				return
			default:
				msg, err := c.consumer.ReadMessage(-1)
				if msg != nil {
					e := newEvent(msg)
					if err = h(e); err != nil {
						logrus.Errorf("handle msg error: %s", err.Error())
					}
				} else {
					logrus.Errorf("consumer error: %v (%v)", err, msg)
					continue
				}

				// commit offset async
				c.commitChan <- msg
			}
		}
	}()

	c.wg.Add(1)
	go func() {
		for m := range c.commitChan {
			if _, err := c.consumer.CommitMessage(m); err != nil {
				logrus.Errorf("commit error: %v (%v)", err, m)
			}
		}

		c.wg.Done()
	}()

	return newSubscriber(c), nil
}

func (c *Confluent) String() string {
	return "kafka"
}

func newSubscriber(c *Confluent) mq.Subscriber {
	return &subscriber{confluent: c}
}

type subscriber struct {
	confluent *Confluent
}

func (s *subscriber) Options() mq.SubscribeOptions {
	return mq.SubscribeOptions{}
}

func (s *subscriber) Topic() string {
	topics, _ := s.confluent.consumer.Subscription()

	return strings.Join(topics, ",")
}

func (s *subscriber) Unsubscribe() error {
	if err := s.confluent.consumer.Unsubscribe(); err != nil {
		return err
	}

	close(s.confluent.stopRead)

	s.confluent.wg.Wait()

	return nil
}
