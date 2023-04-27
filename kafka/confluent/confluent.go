package confluent

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/opensourceways/kafka-lib/mq"
)

type Handlers map[string]mq.Handler

func NewConfluentMQ() mq.MQ {
	return &Confluent{}
}

type Confluent struct {
	producer         *kafka.Producer
	opts             mq.Options
	broker           string
	subscribers      map[string]*subscriber
	groupTopicHandle map[string]Handlers
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

	// the relationship between group,topic and handle
	c.groupTopicHandle = make(map[string]Handlers)

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
	c.producer.Flush(3000)
	c.producer.Close()

	return nil
}

func (c *Confluent) Publish(topic string, msg *mq.Message, opts ...mq.PublishOption) error {
	go func() {
		for e := range c.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return c.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          msg.Body,
		Key:            []byte(msg.MessageKey()),
	}, nil)
}

func (c *Confluent) Subscribe(topic, group string, handler mq.Handler) (mqs mq.Subscriber, err error) {
	// when a group subscribes to multiple topics,
	// we should unsubscribe the previous one
	if s, ok := c.subscribers[group]; ok {
		if err = s.Unsubscribe(); err != nil {
			return
		}
	}

	// new consumer, the relationship between consumer and group is one-to-one
	s, err := newSubscriber(c.broker, group)
	if err != nil {
		return
	}

	// store the relationship, use it for the next subscription
	c.buildGroupTopicHandle(group, topic, handler)

	// the real subscription
	if err = s.subscribe(c.groupTopicHandle[group]); err != nil {
		return
	}

	// start to receive message in goroutine
	s.start()

	// store the subscriber, use it at the beginning of this function
	c.subscribers[group] = s

	mqs = s

	return
}

func (c *Confluent) String() string {
	return "kafka-confluent"
}

func (c *Confluent) buildGroupTopicHandle(group, topic string, handler mq.Handler) {
	handlers, ok := c.groupTopicHandle[group]
	if !ok {
		handlers = make(Handlers)
	}

	handlers[topic] = handler

	c.groupTopicHandle[group] = handlers
}
