package mq

import (
	"context"
	"time"
)

// IDelayedProducer 延迟消息能力（NSQ DeferredPublish / RocketMQ 延迟级别），
// Kafka 不实现。业务通过类型断言启用：
//
//	if p, ok := factory.Producer().(IDelayedProducer); ok { p.SendDelay(...) }
type IDelayedProducer interface {
	SendDelay(ctx context.Context, msg *Message, at time.Time) error
}

// IOrderedConsumer 顺序消费能力（Kafka 单分区 / RocketMQ 顺序队列），NSQ 不实现。
type IOrderedConsumer interface {
	SubscribeOrdered(ctx context.Context, topic, group string, handler Handler) error
}
