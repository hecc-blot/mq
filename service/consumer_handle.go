package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	ioc "github.com/hecc-blot/core/contract/ioc"
	"github.com/hecc-blot/core/contract/log"
	mqContract "github.com/hecc-blot/mq/contract"
)

// consumerEntry 一条消费者注册项。
type consumerEntry struct {
	topic    string
	group    string
	instance mqContract.IConsume
	opts     []mqContract.SubscribeOption
}

// ConsumerHandle 消费者注册器：注册期完成依赖注入，Start 期统一订阅，Shutdown 统一停止。
//
// 注册发生在启动期、单线程，故只在 entries 上按需加锁，不做无锁优化；
// 启动后 entries 不再变化，Start / Shutdown 的加锁仅为保护 started / closed 状态。
type ConsumerHandle struct {
	consumer  mqContract.IConsumer
	container ioc.IContainer
	logger    log.ILog

	mu      sync.Mutex
	entries []*consumerEntry
	started bool
	closed  bool
}

// 编译期断言：确保 ConsumerHandle 实现 IConsumerHandle
var _ mqContract.IConsumerHandle = (*ConsumerHandle)(nil)

// NewConsumerHandle 创建消费者注册器。
//
// consumer 通常传 mqFactory.Consumer()；container 必传，nil 会 panic（对齐 sse 的 NewSseSvc）；
// logger 可为 nil，传 nil 时不输出启动日志。
func NewConsumerHandle(consumer mqContract.IConsumer, container ioc.IContainer, logger log.ILog) mqContract.IConsumerHandle {
	if consumer == nil {
		panic("mq: consumer 不能为空")
	}
	if container == nil {
		panic("ioc: 注入容器不能为空")
	}
	return &ConsumerHandle{consumer: consumer, container: container, logger: logger}
}

// Register 注册一个消费者，并在注册期完成依赖注入（与框架 ApiHandle 注册期注入一致）。
func (h *ConsumerHandle) Register(topic, group string, consumer mqContract.IConsume, opts ...mqContract.SubscribeOption) mqContract.IConsumerHandle {
	if consumer == nil {
		panic("mq: 消费者不能为空")
	}
	if topic == "" || group == "" {
		panic(fmt.Errorf("mq: topic 与 group 不能为空(topic = %q, group = %q)", topic, group))
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.started {
		panic(fmt.Errorf("mq: 消费者注册必须在 Start 之前(topic = %s, group = %s)", topic, group))
	}
	if h.closed {
		panic(fmt.Errorf("mq: 注册器已关闭，不能再注册消费者(topic = %s, group = %s)", topic, group))
	}
	for _, e := range h.entries {
		if e.topic == topic && e.group == group {
			panic(fmt.Errorf("mq: 重复注册同一 topic+group(topic = %s, group = %s)", topic, group))
		}
	}

	h.container.Inject(consumer)
	h.entries = append(h.entries, &consumerEntry{topic: topic, group: group, instance: consumer, opts: opts})
	return h
}

// Start 启动所有已注册消费者。
func (h *ConsumerHandle) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return errors.New("mq: 注册器已关闭，不能再启动")
	}
	if h.started {
		return errors.New("mq: 消费者已启动")
	}

	for _, entry := range h.entries {
		handler := func(ctx context.Context, msg *mqContract.Message) error {
			return entry.instance.Consume(ctx, msg)
		}
		if err := h.consumer.Subscribe(ctx, entry.topic, entry.group, handler, entry.opts...); err != nil {
			// IConsumer.Close 只能整体关闭，无法只回退这一条订阅。
			_ = h.consumer.Close()
			h.closed = true
			return fmt.Errorf("mq: 订阅失败(topic = %s, group = %s): %w", entry.topic, entry.group, err)
		}
		if h.logger != nil {
			h.logger.Info(ctx, "mq消费者已启动", "topic", entry.topic, "group", entry.group)
		}
	}

	h.started = true
	return nil
}

// Shutdown 优雅停止所有消费者。幂等：重复调用直接返回 nil。
func (h *ConsumerHandle) Shutdown() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}
	h.closed = true
	return h.consumer.Close()
}
