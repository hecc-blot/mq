package mqEnum

// Value MQ 后端类型。
type Value string

const (
	// Kafka Apache Kafka 分布式流平台。
	Kafka Value = "kafka"
	// Nsq 轻量级实时分布式消息平台。
	Nsq Value = "nsq"
	// RocketMQ 阿里开源分布式消息中间件。
	RocketMQ Value = "rocketmq"
)
