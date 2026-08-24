package service

import (
	"context"
	"sync/atomic"
	"time"

	mqConf "github.com/hecc-blot/mq/config"
	mqContract "github.com/hecc-blot/mq/contract"

	"github.com/nsqio/go-nsq"
)

// nsqProducer 持有一个 producer 池，按轮询发布。NSQ 的 nsqd 节点相互独立、无复制，
// 生产端需覆盖所有 nsqd 才能保证 topic 的每条消息都能被消费到。
type nsqProducer struct {
	producers []*nsq.Producer
	counter   uint64
}

// Send NSQ 为异步投递：Publish 仅返回本地错误，不等待 nsqd 确认。
// NSQ 无消息头，链路追踪传播不支持（Headers 被忽略）。
func (p *nsqProducer) Send(_ context.Context, msg *mqContract.Message) error {
	idx := atomic.AddUint64(&p.counter, 1) % uint64(len(p.producers))
	return p.producers[idx].Publish(msg.Topic, msg.Body)
}

// SendDelay 延迟投递。NSQ 的 DeferredPublish 支持任意时长（秒级精度），比 RocketMQ
// 的预定义等级更灵活；at 已到点则立即投递。
func (p *nsqProducer) SendDelay(_ context.Context, msg *mqContract.Message, at time.Time) error {
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	idx := atomic.AddUint64(&p.counter, 1) % uint64(len(p.producers))
	return p.producers[idx].DeferredPublish(msg.Topic, delay, msg.Body)
}

func (p *nsqProducer) Close() error {
	for _, prod := range p.producers {
		prod.Stop()
	}
	return nil
}

func newNsqProducer(cfg *mqConf.Nsq) (*nsqProducer, error) {
	producers := make([]*nsq.Producer, 0, len(cfg.Nsqd))
	for _, addr := range cfg.Nsqd {
		prod, err := nsq.NewProducer(addr, nsq.NewConfig())
		if err != nil {
			for _, p := range producers {
				p.Stop()
			}
			return nil, err
		}
		producers = append(producers, prod)
	}
	return &nsqProducer{producers: producers}, nil
}
