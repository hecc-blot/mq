package service

import (
	"context"
	"fmt"
	"time"

	"github.com/hecc-blot/framework/util"
	mqConf "github.com/hecc-blot/mq/config"
	mqContract "github.com/hecc-blot/mq/contract"
	trace "github.com/hecc-blot/trace/contract"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// rocketmqDelayLevels RocketMQ 默认延迟等级（索引即 level，1~18）。
var rocketmqDelayLevels = []time.Duration{
	0,
	1 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	1 * time.Minute,
	2 * time.Minute,
	3 * time.Minute,
	4 * time.Minute,
	5 * time.Minute,
	6 * time.Minute,
	7 * time.Minute,
	8 * time.Minute,
	9 * time.Minute,
	10 * time.Minute,
	20 * time.Minute,
	30 * time.Minute,
	1 * time.Hour,
	2 * time.Hour,
}

// rocketmqDelayLevel 把延迟时长映射到 RocketMQ 预定义等级，向上取整避免提前投递；
// 超 2h 取最大等级。
func rocketmqDelayLevel(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	for i := 1; i < len(rocketmqDelayLevels); i++ {
		if d <= rocketmqDelayLevels[i] {
			return i
		}
	}
	return len(rocketmqDelayLevels) - 1
}

type rocketmqProducer struct {
	producer rocketmq.Producer
	traceSvc trace.ITrace
}

func (p *rocketmqProducer) Send(ctx context.Context, msg *mqContract.Message) error {
	return p.sendSync(ctx, p.buildMessage(ctx, msg))
}

// SendDelay 延迟投递。RocketMQ 仅支持预定义延迟等级（1s~2h），按不小于目标延迟的
// 最小等级映射；at 已到点则立即投递。
func (p *rocketmqProducer) SendDelay(ctx context.Context, msg *mqContract.Message, at time.Time) error {
	pm := p.buildMessage(ctx, msg)
	if level := rocketmqDelayLevel(time.Until(at)); level > 0 {
		pm = pm.WithDelayTimeLevel(level)
	}
	return p.sendSync(ctx, pm)
}

// buildMessage 构造 RocketMQ 消息并注入链路追踪到 Properties。
func (p *rocketmqProducer) buildMessage(ctx context.Context, msg *mqContract.Message) *primitive.Message {
	ctx = util.ExtractContext(ctx)
	if p.traceSvc != nil {
		if msg.Headers == nil {
			msg.Headers = make(map[string]string)
		}
		_ = p.traceSvc.Inject(ctx, msg.Headers)
	}

	pm := primitive.NewMessage(msg.Topic, msg.Body)
	if msg.Key != "" {
		pm = pm.WithKeys([]string{msg.Key})
	}
	if len(msg.Headers) > 0 {
		pm.WithProperties(msg.Headers)
	}
	return pm
}

func (p *rocketmqProducer) sendSync(ctx context.Context, pm *primitive.Message) error {
	res, err := p.producer.SendSync(ctx, pm)
	if err != nil {
		return err
	}
	if res.Status != primitive.SendOK {
		return fmt.Errorf("mq: rocketmq 发送失败, status=%v", res.Status)
	}
	return nil
}

func (p *rocketmqProducer) Close() error {
	return p.producer.Shutdown()
}

func newRocketmqProducer(cfg *mqConf.RocketMQ, traceSvc trace.ITrace) (*rocketmqProducer, error) {
	opts := []producer.Option{
		producer.WithNameServer(primitive.NamesrvAddr(cfg.NameServer)),
		producer.WithGroupName(cfg.ProducerGroup),
	}
	if cfg.AccessKey != "" {
		opts = append(opts, producer.WithCredentials(primitive.Credentials{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
		}))
	}

	p, err := rocketmq.NewProducer(opts...)
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	return &rocketmqProducer{producer: p, traceSvc: traceSvc}, nil
}
