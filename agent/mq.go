package agent

import (
	"errors"

	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/confluent"
	"github.com/opensourceways/kafka-lib/mq"
)

var instance *serviceImpl

func Init(cfg *Config, log *logrus.Entry) error {
	err := confluent.Init(
		mq.Addresses(cfg.mqConfig().Addresses...),
		mq.Log(log),
	)
	if err != nil {
		return err
	}

	if err := confluent.Connect(); err != nil {
		return err
	}

	instance = &serviceImpl{}

	return nil
}

func Exit() {
	if instance != nil {
		instance.unsubscribe()
	}

	if err := confluent.Disconnect(); err != nil {
		logrus.Errorf("exit kafka, err:%v", err)
	}
}

func Publish(topic string, header map[string]string, msg []byte) error {
	return confluent.Publish(topic, &mq.Message{
		Header: header,
		Body:   msg,
	})
}

func Subscribe(group string, handlers map[string]Handler) error {
	if instance == nil {
		return errors.New("unimplemented")
	}

	return instance.subscribe(group, handlers)
}

// Handler
type Handler func([]byte, map[string]string) error

// serviceImpl
type serviceImpl struct {
	subscribers []mq.Subscriber
}

func (impl *serviceImpl) unsubscribe() {
	s := impl.subscribers
	for i := range s {
		if err := s[i].Unsubscribe(); err != nil {
			logrus.Errorf(
				"failed to unsubscribe to topic:%s, err:%v",
				s[i].Topic(), err,
			)
		}
	}
}

func (impl *serviceImpl) subscribe(group string, handlers map[string]Handler) error {
	for topic, h := range handlers {
		s, err := impl.registerHandler(topic, group, h)
		if err != nil {
			return err
		}

		if s != nil {
			impl.subscribers = append(impl.subscribers, s)
		} else {
			logrus.Infof("does not subscribe topic:%s", topic)
		}
	}

	return nil
}

func (impl *serviceImpl) registerHandler(topic, group string, h Handler) (mq.Subscriber, error) {
	if h == nil {
		return nil, nil
	}

	return confluent.Subscribe(topic, group+topic, func(e mq.Event) error {
		msg := e.Message()
		if msg == nil {
			return nil
		}

		return h(msg.Body, msg.Header)
	})
}
