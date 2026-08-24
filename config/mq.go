package config

import mqEnum "github.com/hecc-blot/mq/enum/mq"

const (
	// DefaultConcurrency 默认消费并发数。
	DefaultConcurrency = 1
	// DefaultProducerGroup 默认生产者组名。
	DefaultProducerGroup = "mq-producer"
)

// Config MQ 配置。
type Config struct {
	// Driver 后端类型，默认 Kafka。
	Driver   mqEnum.Value `mapstructure:"driver"`
	Kafka    Kafka
	Nsq      Nsq
	RocketMQ RocketMQ
	// Consumer 消费通用配置，各后端共享。
	Consumer Consumer
}

// Kafka Kafka 后端配置。
type Kafka struct {
	// Brokers broker 地址列表，如 ["127.0.0.1:9092"]。
	Brokers []string `mapstructure:"brokers"`
}

// Nsq NSQ 后端配置。
type Nsq struct {
	// Nsqd nsqd tcp 地址列表，如 ["127.0.0.1:4150"]。生产端必填。
	Nsqd []string `mapstructure:"nsqd"`
	// Nsqlookupd nsqlookupd http 地址列表，用于服务发现，如 ["127.0.0.1:4161"]。
	Nsqlookupd []string `mapstructure:"nsqlookupd"`
}

// RocketMQ RocketMQ 后端配置。
type RocketMQ struct {
	// NameServer NameServer 地址列表，如 ["127.0.0.1:9876"]。
	NameServer []string `mapstructure:"name_server"`
	// ProducerGroup 生产者组名，默认 mq-producer。
	ProducerGroup string `mapstructure:"producer_group"`
	// AccessKey / SecretKey ACL 认证，为空则不启用。
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
}

// Consumer 消费通用配置。
type Consumer struct {
	// Concurrency 消费并发数，默认 1。
	Concurrency int `mapstructure:"concurrency"`
}

// Normalize 补全默认值，供工厂构造各后端时调用。
func Normalize(cfg Config) Config {
	if cfg.Driver == "" {
		cfg.Driver = mqEnum.Kafka
	}
	if cfg.Consumer.Concurrency <= 0 {
		cfg.Consumer.Concurrency = DefaultConcurrency
	}
	if cfg.RocketMQ.ProducerGroup == "" {
		cfg.RocketMQ.ProducerGroup = DefaultProducerGroup
	}
	return cfg
}
