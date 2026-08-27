package service

import (
	"context"
	"errors"
	"sync"

	"github.com/hecc-blot/core/contract/log"
	"github.com/hecc-blot/framework/util"
	mqConf "github.com/hecc-blot/mq/config"
	mqContract "github.com/hecc-blot/mq/contract"

	"github.com/nsqio/go-nsq"
)

type nsqConsumer struct {
	nsqd        []string
	nsqlookupd  []string
	concurrency int
	logger      log.ILog

	mu        sync.Mutex
	consumers []*nsq.Consumer
}

func (c *nsqConsumer) Subscribe(ctx context.Context, topic, group string, handler mqContract.Handler, opts ...mqContract.SubscribeOption) error {
	if handler == nil {
		return errors.New("mq: handler 不能为空")
	}
	ctx = util.ExtractContext(ctx)

	n := c.concurrency
	if sub := mqContract.ApplySubscribeOptions(opts...); sub.Concurrency > 0 {
		n = sub.Concurrency
	}

	cfg := nsq.NewConfig()
	cfg.MaxInFlight = n // NSQ 并发由 MaxInFlight 控制

	// group 即 NSQ 的 channel。
	consumer, err := nsq.NewConsumer(topic, group, cfg)
	if err != nil {
		return err
	}

	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		return c.handle(ctx, topic, m, handler)
	}))

	for _, addr := range c.nsqlookupd {
		if err := consumer.ConnectToNSQLookupd(addr); err != nil {
			return err
		}
	}
	for _, addr := range c.nsqd {
		if err := consumer.ConnectToNSQD(addr); err != nil {
			return err
		}
	}

	c.mu.Lock()
	c.consumers = append(c.consumers, consumer)
	c.mu.Unlock()
	return nil
}

func (c *nsqConsumer) handle(ctx context.Context, topic string, m *nsq.Message, handler mqContract.Handler) error {
	// NSQ 无消息头，无法提取上游 span，链路在消费端断点。
	msg := &mqContract.Message{Topic: topic, Body: m.Body}

	handlerErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				c.logger.Error(ctx, "mq消费handler panic", "topic", topic, "panic", r)
				// 不立即重投，交给 NSQ 原生退避。
				_ = msg.Nack(ctx, nil, false)
			}
		}()
		return handler(ctx, msg)
	}()

	acked, _ := msg.Finalize(handlerErr)
	if acked {
		return nil // go-nsq 自动 FIN
	}
	return errors.New("mq: 消费失败") // go-nsq 自动 REQ（原生退避）
}

func (c *nsqConsumer) Close() error {
	c.mu.Lock()
	consumers := c.consumers
	c.consumers = nil
	c.mu.Unlock()

	for _, con := range consumers {
		con.Stop()
	}
	return nil
}

func newNsqConsumer(cfg *mqConf.Nsq, consumerCfg *mqConf.Consumer, logger log.ILog) *nsqConsumer {
	return &nsqConsumer{
		nsqd:        cfg.Nsqd,
		nsqlookupd:  cfg.Nsqlookupd,
		concurrency: consumerCfg.Concurrency,
		logger:      logger,
	}
}
