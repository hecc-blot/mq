package service

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/hecc-blot/core/contract/log"
	"github.com/hecc-blot/framework/util"
	mqConf "github.com/hecc-blot/mq/config"
	mqContract "github.com/hecc-blot/mq/contract"
	trace "github.com/hecc-blot/trace/contract"

	"github.com/segmentio/kafka-go"
)

type kafkaConsumer struct {
	brokers     []string
	concurrency int
	logger      log.ILog
	traceSvc    trace.ITrace

	mu      sync.Mutex
	readers []*kafka.Reader
	wg      sync.WaitGroup
}

func (c *kafkaConsumer) Subscribe(ctx context.Context, topic, group string, handler mqContract.Handler, opts ...mqContract.SubscribeOption) error {
	if handler == nil {
		return errors.New("mq: handler 不能为空")
	}
	ctx = util.ExtractContext(ctx)

	n := c.concurrency
	if sub := mqContract.ApplySubscribeOptions(opts...); sub.Concurrency > 0 {
		n = sub.Concurrency
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < n; i++ {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     c.brokers,
			GroupID:     group,
			GroupTopics: []string{topic},
			MinBytes:    10e3, // 10KB
			MaxBytes:    10e6, // 10MB
		})
		c.readers = append(c.readers, reader)
		c.wg.Add(1)
		go c.runReader(ctx, reader, handler)
	}
	return nil
}

// SubscribeOrdered 顺序消费：单 Reader 串行拉取处理，保证单分区内按 offset 有序。
// 全局有序需配合生产端 Key 哈希路由到单分区（keyAwareBalancer 已保证同 Key 同分区）。
func (c *kafkaConsumer) SubscribeOrdered(ctx context.Context, topic, group string, handler mqContract.Handler) error {
	return c.Subscribe(ctx, topic, group, handler, mqContract.WithConcurrency(1))
}

func (c *kafkaConsumer) Close() error {
	c.mu.Lock()
	readers := c.readers
	c.readers = nil
	c.mu.Unlock()

	for _, r := range readers {
		_ = r.Close()
	}
	c.wg.Wait()
	return nil
}

// runReader 单个 Reader 的消费循环：Reader 内串行拉取，同组多个 Reader 由 Kafka
// 分配分区，保证单分区内有序。
func (c *kafkaConsumer) runReader(ctx context.Context, reader *kafka.Reader, handler mqContract.Handler) {
	defer c.wg.Done()
	for {
		km, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			c.logger.Error(ctx, "mq消费拉取消息失败", "error", err)
			continue
		}
		c.handle(ctx, reader, km, handler)
	}
}

func (c *kafkaConsumer) handle(ctx context.Context, reader *kafka.Reader, km kafka.Message, handler mqContract.Handler) {
	headers := fromKafkaHeaders(km.Headers)
	msg := &mqContract.Message{
		Topic:   km.Topic,
		Key:     string(km.Key),
		Body:    km.Value,
		Headers: headers,
	}

	hCtx := c.extractTrace(headers)

	// 单个 handler 用 recover 兜底，避免 panic 中断整个消费循环。
	handlerErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				c.logger.Error(ctx, "mq消费handler panic", "topic", km.Topic, "panic", r)
				// 不立即重投，避免毒消息死循环；重平衡后重投。
				_ = msg.Nack(ctx, nil, false)
			}
		}()
		return handler(hCtx, msg)
	}()

	acked, requeue := msg.Finalize(handlerErr)
	switch {
	case acked:
		if err := reader.CommitMessages(ctx, km); err != nil {
			c.logger.Error(ctx, "mq提交offset失败", "topic", km.Topic, "error", err)
		}
	case requeue:
		if err := reader.SetOffset(km.Offset); err != nil {
			c.logger.Error(ctx, "mq回退offset失败", "topic", km.Topic, "error", err)
		}
	}
	// requeue=false：既不提交也不回退，重平衡/重启后重投。
}

// extractTrace 从消息头提取上游 span，串起 生产→MQ→消费 的链路。
func (c *kafkaConsumer) extractTrace(headers map[string]string) context.Context {
	ctx := context.Background()
	if c.traceSvc != nil && len(headers) > 0 {
		if extracted, err := c.traceSvc.Extract(headers); err == nil && extracted != nil {
			ctx = extracted
		}
	}
	return ctx
}

func newKafkaConsumer(cfg *mqConf.Kafka, consumerCfg *mqConf.Consumer, logger log.ILog, traceSvc trace.ITrace) *kafkaConsumer {
	return &kafkaConsumer{
		brokers:     cfg.Brokers,
		concurrency: consumerCfg.Concurrency,
		logger:      logger,
		traceSvc:    traceSvc,
	}
}

// toKafkaHeaders map[string]string → []kafka.Header。
func toKafkaHeaders(headers map[string]string) []kafka.Header {
	if len(headers) == 0 {
		return nil
	}
	h := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		h = append(h, kafka.Header{Key: k, Value: []byte(v)})
	}
	return h
}

// fromKafkaHeaders []kafka.Header → map[string]string。
func fromKafkaHeaders(headers []kafka.Header) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
