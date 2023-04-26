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

	consumers map[string]*subscriber
}

func (c *Confluent) Init(opts ...mq.Option) error {
	for _, o := range opts {
		o(&c.opts)
	}

	if c.opts.Addresses == nil {
		c.opts.Addresses = []string{"127.0.0.1:9092"}
	}

	c.broker = strings.Join(c.opts.Addresses, ",")

	c.consumers = make(map[string]*subscriber)

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
	s, err := c.newSubscriber(group)
	if err != nil {
		return
	}

	s.topics.Insert(topic)
	s.handlers[topic] = handler

	if err = s.consumer.SubscribeTopics(s.topics.UnsortedList(), nil); err != nil {
		return
	}

	s.start()

	mqs = s

	return
}

func (c *Confluent) String() string {
	return "kafka"
}

func (c *Confluent) newSubscriber(group string) (sub *subscriber, err error) {
	if s, ok := c.consumers[group]; ok {
		return s, nil
	}

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

	sub = &subscriber{
		consumer:   consumer,
		commitChan: make(chan *kafka.Message, 100),
		stopRead:   make(chan struct{}),

		handlers: make(map[string]mq.Handler),
	}

	return
}
