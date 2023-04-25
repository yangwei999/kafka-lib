package confluent

import (
	"errors"
	"strings"
	"time"

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

	consumers map[string]struct{}
}

func (c *Confluent) Init(opts ...mq.Option) error {
	for _, o := range opts {
		o(&c.opts)
	}

	if c.opts.Addresses == nil {
		c.opts.Addresses = []string{"127.0.0.1:9092"}
	}

	c.broker = strings.Join(c.opts.Addresses, ",")

	c.consumers = make(map[string]struct{})

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

func (c *Confluent) Subscribe(topic, group string, handler mq.Handler) (s mq.Subscriber, err error) {
	if _, ok := c.consumers[group]; ok {
		err = errors.New("the group already exists, we don't recommend a group subscribe multiple topics")

		return
	}

	ns, err := newSubscriber(c.broker, topic, group, handler)
	if err != nil {
		return
	}

	ns.start()

	c.consumers[group] = struct{}{}

	s = ns

	return
}

func (c *Confluent) String() string {
	return "kafka"
}
