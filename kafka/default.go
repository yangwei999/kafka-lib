package kafka

import (
	"github.com/opensourceways/kafka-lib/kafka/confluent"
	"github.com/opensourceways/kafka-lib/kafka/sarama"
	"github.com/opensourceways/kafka-lib/mq"
)

var (
	DefaultMQ mq.MQ
)

func Init(opts ...mq.Option) error {
	DefaultMQ = sarama.NewSaramaMQ()

	return DefaultMQ.Init(opts...)
}

func InitV2(opts ...mq.Option) error {
	DefaultMQ = confluent.NewConfluentMQ()

	return DefaultMQ.Init(opts...)
}

func Connect() error {
	return DefaultMQ.Connect()
}

func Disconnect() error {
	return DefaultMQ.Disconnect()
}

func Publish(topic string, msg *mq.Message, opts ...mq.PublishOption) error {
	return DefaultMQ.Publish(topic, msg, opts...)
}

func Subscribe(topic, name string, handler mq.Handler) (mq.Subscriber, error) {
	return DefaultMQ.Subscribe(topic, name, handler)
}

func String() string {
	return DefaultMQ.String()
}
