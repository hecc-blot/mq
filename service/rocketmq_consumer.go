package service

import (
	"context"
	"errors"
	"sync"

	"github.com/hecc-blot/framework/contract/log"
	"github.com/hecc-blot/framework/util"
	mqConf "github.com/hecc-blot/mq/config"
	mqContract "github.com/hecc-blot/mq/contract"
	trace "github.com/hecc-blot/trace/contract"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type rocketmqConsumer struct {
	nameServer []string
	accessKey  string
	secretKey  string
	logger     log.ILog
	traceSvc   trace.ITrace

	mu        sync.Mutex
	consumers []rocketmq.PushConsumer
}

func (c *rocketmqConsumer) Subscribe(ctx context.Context, topic, group string, handler mqContract.Handler, opts ...mqContract.SubscribeOption) error {
	// 并发由 RocketMQ 队列驱动，SubscribeOption 暂不适用。
	return c.subscribe(ctx, topic, group, handler, false)
}

// SubscribeOrdered 顺序消费：同一 MessageQueue 内串行，消费失败挂起队列直至重试成功。
func (c *rocketmqConsumer) SubscribeOrdered(ctx context.Context, topic, group string, handler mqContract.Handler) error {
	return c.subscribe(ctx, topic, group, handler, true)
}

func (c *rocketmqConsumer) subscribe(ctx context.Context, topic, group string, handler mqContract.Handler, order bool) error {
	if handler == nil {
		return errors.New("mq: handler 不能为空")
	}
	ctx = util.ExtractContext(ctx)

	popts := []consumer.Option{
		consumer.WithNameServer(primitive.NamesrvAddr(c.nameServer)),
		consumer.WithGroupName(group),
		consumer.WithConsumeMessageBatchMaxSize(1), // 单条批，映射 per-message handler
	}
	if order {
		popts = append(popts, consumer.WithConsumerOrder(true))
	}
	if c.accessKey != "" {
		popts = append(popts, consumer.WithCredentials(primitive.Credentials{
			AccessKey: c.accessKey,
			SecretKey: c.secretKey,
		}))
	}

	pc, err := rocketmq.NewPushConsumer(popts...)
	if err != nil {
		return err
	}

	if err := pc.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		return c.handle(ctx, topic, msgs, handler)
	}); err != nil {
		return err
	}

	if err := pc.Start(); err != nil {
		return err
	}

	c.mu.Lock()
	c.consumers = append(c.consumers, pc)
	c.mu.Unlock()
	return nil
}

// handle 批内逐条处理，任一失败则整批重投（至少一次语义）。
func (c *rocketmqConsumer) handle(ctx context.Context, topic string, msgs []*primitive.MessageExt, handler mqContract.Handler) (consumer.ConsumeResult, error) {
	result := consumer.ConsumeSuccess
	for _, m := range msgs {
		msg := &mqContract.Message{
			Topic:   topic,
			Key:     m.GetKeys(),
			Body:    m.Body,
			Headers: m.GetProperties(),
		}
		hCtx := c.extractTrace(msg.Headers)

		handlerErr := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					c.logger.Error(ctx, "mq消费handler panic", "topic", topic, "panic", r)
					// 交给 RocketMQ 重投 + broker 退避/死信。
					_ = msg.Nack(ctx, nil, true)
				}
			}()
			return handler(hCtx, msg)
		}()

		acked, _ := msg.Finalize(handlerErr)
		if !acked {
			// RocketMQ 的 requeue 区分由 broker 重试次数与死信队列统一处理，
			// 客户端层面统一 ConsumeRetryLater。
			result = consumer.ConsumeRetryLater
		}
	}
	return result, nil
}

func (c *rocketmqConsumer) extractTrace(headers map[string]string) context.Context {
	ctx := context.Background()
	if c.traceSvc != nil && len(headers) > 0 {
		if extracted, err := c.traceSvc.Extract(headers); err == nil && extracted != nil {
			ctx = extracted
		}
	}
	return ctx
}

func (c *rocketmqConsumer) Close() error {
	c.mu.Lock()
	consumers := c.consumers
	c.consumers = nil
	c.mu.Unlock()

	for _, pc := range consumers {
		_ = pc.Shutdown()
	}
	return nil
}

func newRocketmqConsumer(cfg *mqConf.RocketMQ, logger log.ILog, traceSvc trace.ITrace) *rocketmqConsumer {
	return &rocketmqConsumer{
		nameServer: cfg.NameServer,
		accessKey:  cfg.AccessKey,
		secretKey:  cfg.SecretKey,
		logger:     logger,
		traceSvc:   traceSvc,
	}
}
