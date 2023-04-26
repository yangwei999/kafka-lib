package confluent

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/opensourceways/kafka-lib/mq"
)

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
