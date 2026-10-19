package service

import (
	"fmt"

	"github.com/hecc-blot/core/contract/log"
	mqConf "github.com/hecc-blot/mq/config"
	mqContract "github.com/hecc-blot/mq/contract"
	mqEnum "github.com/hecc-blot/mq/enum/mq"
	trace "github.com/hecc-blot/trace/contract"
)

// Factory MQ 工厂实现，按配置的 Driver 构建后端。
type Factory struct {
	producer mqContract.IProducer
	consumer mqContract.IConsumer
}

func (f *Factory) Producer() mqContract.IProducer { return f.producer }
func (f *Factory) Consumer() mqContract.IConsumer { return f.consumer }

// NewMqFactory 按配置构建 MQ 工厂。返回 cleanup 用于关闭底层连接。
// traceSvc 可传 nil，传 nil 时不开启链路追踪传播。
func NewMqFactory(cfg *mqConf.Config, logger log.ILog, traceSvc trace.ITrace) (mqContract.IMqFactory, func(), error) {
	if cfg == nil {
		return nil, func() {}, fmt.Errorf("mq: config must not be nil")
	}
	c := mqConf.Normalize(*cfg)

	switch c.Driver {
	case mqEnum.Kafka:
		if len(c.Kafka.Brokers) == 0 {
			return nil, func() {}, fmt.Errorf("mq: kafka brokers is not configured")
		}
		if !c.Kafka.StartOffset.Valid() {
			return nil, func() {}, fmt.Errorf("mq: invalid kafka start_offset: %s (must be first or last)", c.Kafka.StartOffset)
		}
		producer := newKafkaProducer(&c.Kafka, traceSvc)
		consumer := newKafkaConsumer(&c.Kafka, &c.Consumer, logger, traceSvc)
		return &Factory{producer: producer, consumer: consumer}, func() {
			_ = producer.Close()
			_ = consumer.Close()
		}, nil
	case mqEnum.Nsq:
		if len(c.Nsq.Nsqd) == 0 {
			return nil, func() {}, fmt.Errorf("mq: nsqd address is not configured for nsq")
		}
		producer, err := newNsqProducer(&c.Nsq)
		if err != nil {
			return nil, func() {}, err
		}
		consumer := newNsqConsumer(&c.Nsq, &c.Consumer, logger)
		return &Factory{producer: producer, consumer: consumer}, func() {
			_ = producer.Close()
			_ = consumer.Close()
		}, nil
	case mqEnum.RocketMQ:
		if len(c.RocketMQ.NameServer) == 0 {
			return nil, func() {}, fmt.Errorf("mq: name_server is not configured for rocketmq")
		}
		producer, err := newRocketmqProducer(&c.RocketMQ, traceSvc)
		if err != nil {
			return nil, func() {}, err
		}
		consumer := newRocketmqConsumer(&c.RocketMQ, logger, traceSvc)
		return &Factory{producer: producer, consumer: consumer}, func() {
			_ = producer.Close()
			_ = consumer.Close()
		}, nil
	default:
		return nil, func() {}, fmt.Errorf("mq: unsupported backend type: %v", c.Driver)
	}
}
