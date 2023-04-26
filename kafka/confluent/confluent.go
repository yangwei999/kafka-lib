package confluent

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/opensourceways/kafka-lib/mq"
)

func NewConfluentMQ() mq.MQ {
	return &Confluent{}
}

type Confluent struct {
	producer *kafka.Producer
	opts     mq.Options
	broker   string

	subscribers map[string]*subscriber

	handlers map[string]mq.Handler
}

func (c *Confluent) Init(opts ...mq.Option) error {
	for _, o := range opts {
		o(&c.opts)
	}

	if c.opts.Addresses == nil {
		c.opts.Addresses = []string{"127.0.0.1:9092"}
	}

	c.broker = strings.Join(c.opts.Addresses, ",")

	c.subscribers = make(map[string]*subscriber)

	c.handlers = make(map[string]mq.Handler)

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
	}, nil)
}

func (c *Confluent) Subscribe(topic, group string, handler mq.Handler) (mqs mq.Subscriber, err error) {
	if s, ok := c.subscribers[group]; ok {
		s.Unsubscribe()
	}

	s, err := newSubscriber(c.broker, group)
	if err != nil {
		return
	}

	c.handlers[topic] = handler

	for t, h := range c.handlers {
		s.topics.Insert(t)
		s.handlers[t] = h
	}

	if err = s.consumer.SubscribeTopics(s.topics.UnsortedList(), nil); err != nil {
		return
	}

	s.start()

	c.subscribers[group] = s

	mqs = s

	return
}

func (c *Confluent) String() string {
	return "kafka"
}
