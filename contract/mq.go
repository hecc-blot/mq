package mq

import (
	"context"
	"sync/atomic"
)

// Message 统一消息体，屏蔽各 MQ 后端的消息差异。
//
// 生产侧直接构造 &Message{Topic: ..., Key: ..., Body: ...} 即可；
// 消费侧由适配器构造并注入给 handler。
type Message struct {
	Topic   string            // 主题
	Key     string            // 分区键（Kafka/RocketMQ 有效，NSQ 忽略）
	Body    []byte            // 消息体，序列化由业务自行处理
	Headers map[string]string // 消息头，承载链路追踪 carrier 与业务元数据（NSQ 无头，忽略）

	settled atomic.Bool // 是否已结算
	nacked  atomic.Bool // 结算方向：true=拒绝，false=确认
	requeue bool        // 拒绝时是否立即重投
}

// Ack 确认消费成功。幂等：首次结算生效，重复调用无副作用。
// ctx 为后续异步结算预留，当前实现忽略。
func (m *Message) Ack(_ context.Context) error {
	if !m.settled.CompareAndSwap(false, true) {
		return nil
	}
	m.nacked.Store(false)
	return nil
}

// Nack 消费失败。requeue=true 立即重投，false 退避/进死信。幂等：首次结算生效。
func (m *Message) Nack(_ context.Context, _ error, requeue bool) error {
	if !m.settled.CompareAndSwap(false, true) {
		return nil
	}
	m.nacked.Store(true)
	m.requeue = requeue
	return nil
}

// Finalize 应用自动结算规则并返回最终结算，供适配器决定底层 ack/requeue 行为。
// 未显式结算时按 handlerErr 自动结算：nil→确认，非 nil→拒绝并立即重投。
// 返回 acked=true 表示确认；acked=false 时 requeue 表示是否立即重投。
func (m *Message) Finalize(handlerErr error) (acked, requeue bool) {
	if !m.settled.Load() {
		if handlerErr == nil {
			_ = m.Ack(context.Background())
		} else {
			_ = m.Nack(context.Background(), handlerErr, true)
		}
	}
	return !m.nacked.Load(), m.requeue
}

// IProducer 消息生产者。
type IProducer interface {
	// Send 发送一条消息（NSQ 后端内部为异步投递）。
	Send(ctx context.Context, msg *Message) error
	Close() error
}

// IConsumer 消息消费者。
type IConsumer interface {
	// Subscribe 订阅主题。group 为消费者组/频道。
	// handler 返回 nil 表示消费成功，返回 error 表示失败（默认立即重投）；
	// 也可在 handler 内调用 msg.Ack/Nack 精细控制结算，显式结算优先于返回值。
	Subscribe(ctx context.Context, topic, group string, handler Handler, opts ...SubscribeOption) error
	// Close 优雅关闭：停止接收新消息，等待在途消息处理完成。
	Close() error
}

// Handler 消费回调。
type Handler func(ctx context.Context, msg *Message) error

// IMqFactory MQ 工厂，按配置的 Driver 构建后端。
type IMqFactory interface {
	Producer() IProducer
	Consumer() IConsumer
}

// SubscribeConfig 订阅级配置。
type SubscribeConfig struct {
	// Concurrency 消费并发数。0 表示使用工厂默认值。
	Concurrency int
}

// SubscribeOption 订阅配置项。
type SubscribeOption func(*SubscribeConfig)

// WithConcurrency 设置订阅并发数。
func WithConcurrency(n int) SubscribeOption {
	return func(c *SubscribeConfig) { c.Concurrency = n }
}

// ApplySubscribeOptions 应用订阅配置项。
func ApplySubscribeOptions(opts ...SubscribeOption) SubscribeConfig {
	cfg := SubscribeConfig{Concurrency: 0}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
