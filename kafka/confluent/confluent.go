package confluent

import (
	"errors"
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
	opts     mq.Options
	broker   string

	commitChan chan *kafka.Message
	stopRead   chan struct{}

	consumers map[string]*kafka.Consumer

	wg sync.WaitGroup
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

	c.consumers = make(map[string]*kafka.Consumer)

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

func (c *Confluent) getConsumer() {

}

func (c *Confluent) Subscribe(topic, group string, handler mq.Handler) (s mq.Subscriber, err error) {
	if _, ok := c.consumers[group]; ok {
		err = errors.New("the group already exists, we don't recommend a group subscribe multiple topics")

		return
	}

	c.consumers[group], err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        c.broker,
		"group.id":                 group,
		"auto.offset.reset":        "earliest",
		"allow.auto.create.topics": true,
		"enable.auto.commit":       false,
	})
	if err != nil {
		return
	}

	if err = c.consumers[group].Subscribe(topic, nil); err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-c.stopRead:
				close(c.commitChan)

				return
			default:
				msg, err := c.consumers[group].ReadMessage(-1)
				if msg != nil {
					e := newEvent(msg)
					if err = handler(e); err != nil {
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
			if _, err := c.consumers[group].CommitMessage(m); err != nil {
				logrus.Errorf("commit error: %v (%v)", err, m)
			}
		}

		c.wg.Done()
	}()

	return newSubscriber(c, group), nil
}

func (c *Confluent) String() string {
	return "kafka"
}

func newSubscriber(c *Confluent, group string) mq.Subscriber {
	return &subscriber{confluent: c, group: group}
}

type subscriber struct {
	confluent *Confluent
	group     string
}

func (s *subscriber) Options() mq.SubscribeOptions {
	return mq.SubscribeOptions{}
}

func (s *subscriber) Topic() string {
	topics, _ := s.confluent.consumers[s.group].Subscription()

	return strings.Join(topics, ",")
}

func (s *subscriber) Unsubscribe() error {
	close(s.confluent.stopRead)

	s.confluent.wg.Wait()

	return s.confluent.consumers[s.group].Close()
}
