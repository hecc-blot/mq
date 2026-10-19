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

// OffsetPolicy 新消费者组首次订阅时的起始位置策略。
// 仅在 group 尚无已提交 offset 时生效；已有 offset 的组以已提交 offset 为准。
type OffsetPolicy string

const (
	// FirstOffset 从最早的消息开始消费，用于回放历史 / 补数据。
	FirstOffset OffsetPolicy = "first"
	// LastOffset 从最新的消息开始消费，不回放历史（模块默认）。
	// 与 Kafka auto.offset.reset=latest、RocketMQ 的 CONSUME_FROM_LAST_OFFSET、
	// NSQ 新建 channel 从末尾开始的行为保持一致。
	LastOffset OffsetPolicy = "last"
)

// Valid 策略是否为受支持的值。
func (p OffsetPolicy) Valid() bool {
	return p == FirstOffset || p == LastOffset
}
