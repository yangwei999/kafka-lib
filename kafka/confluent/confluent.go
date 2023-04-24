package confluent

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/mq"
)

func NewConfluentMQ(opts ...mq.Option) mq.MQ {
	options := mq.Options{
		Codec:   mq.JsonCodec{},
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	if len(options.Addresses) == 0 {
		options.Addresses = []string{"127.0.0.1:9092"}
	}

	if options.Log == nil {
		options.Log = logrus.New().WithField("function", "kafka mq")
	}

	return &Confluent{
		opts: options,
	}
}

type Confluent struct {
	producer *kafka.Producer
	opts     mq.Options
}

func (c *Confluent) Init(opts ...mq.Option) error {
	for _, o := range opts {
		o(&c.opts)
	}

	if c.opts.Addresses == nil {
		c.opts.Addresses = []string{"127.0.0.1:9092"}
	}

	if c.opts.Context == nil {
		c.opts.Context = context.Background()
	}

	if c.opts.Codec == nil {
		c.opts.Codec = mq.JsonCodec{}
	}

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
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": c.opts.Addresses[0]})
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
	}, nil)
}

func (c *Confluent) Subscribe(topic, group string, h mq.Handler) (s mq.Subscriber, err error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        c.opts.Addresses[0],
		"group.id":                 group,
		"auto.offset.reset":        "earliest",
		"allow.auto.create.topics": true,
	})
	if err != nil {
		return
	}

	if err = consumer.Subscribe(topic, nil); err != nil {
		return
	}

	go func() {
		run := true
		for run {
			msg, err := consumer.ReadMessage(time.Second)
			if err != nil && !err.(kafka.Error).IsTimeout() {
				fmt.Println(err)
				return
			}

			e := newEvent(msg)
			if err = h(e); err != nil {
				logrus.Errorf("handle msg error: %s", err.Error())
			}

			time.Sleep(time.Second)
		}

	}()

	return newSubscriber(consumer), nil
}

func (c *Confluent) String() string {
	return "kafka"
}

type event struct {
	msg *kafka.Message
}

func newEvent(msg *kafka.Message) mq.Event {
	return &event{msg: msg}
}

func (e *event) Topic() string {
	return *e.msg.TopicPartition.Topic
}

func (e *event) Message() *mq.Message {
	header := make(map[string]string)
	for _, v := range e.msg.Headers {
		if v.Value != nil {
			header[v.Key] = string(v.Value)
		}
	}

	return &mq.Message{
		Key:    string(e.msg.Key),
		Header: header,
		Body:   e.msg.Value,
	}
}

func (e *event) Ack() error {
	return nil
}

func (e *event) Error() error {
	return nil
}

func (e *event) Extra() map[string]interface{} {
	return nil
}

func newSubscriber(c *kafka.Consumer) mq.Subscriber {
	return &subscriber{consumer: c}
}

type subscriber struct {
	consumer *kafka.Consumer
}

func (s *subscriber) Options() mq.SubscribeOptions {
	return mq.SubscribeOptions{}
}

func (s *subscriber) Topic() string {
	return ""
}

func (s *subscriber) Unsubscribe() error {
	return s.consumer.Unsubscribe()
}
