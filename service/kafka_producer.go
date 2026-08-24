package service

import (
	"context"

	"github.com/hecc-blot/framework/util"
	mqConf "github.com/hecc-blot/mq/config"
	mqContract "github.com/hecc-blot/mq/contract"
	trace "github.com/hecc-blot/trace/contract"

	"github.com/segmentio/kafka-go"
)

// keyAwareBalancer 有 Key 走哈希分区（保证同 Key 有序），无 Key 走轮询分散。
type keyAwareBalancer struct {
	rr *kafka.RoundRobin
}

func (b *keyAwareBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if len(msg.Key) > 0 {
		return (&kafka.Hash{}).Balance(msg, partitions...)
	}
	return b.rr.Balance(msg, partitions...)
}

type kafkaProducer struct {
	writer   *kafka.Writer
	traceSvc trace.ITrace
}

func (p *kafkaProducer) Send(ctx context.Context, msg *mqContract.Message) error {
	ctx = util.ExtractContext(ctx)

	// 链路追踪传播：把当前 span 注入消息头，消费端可 Extract 串起链路。
	if p.traceSvc != nil {
		if msg.Headers == nil {
			msg.Headers = make(map[string]string)
		}
		_ = p.traceSvc.Inject(ctx, msg.Headers)
	}

	km := kafka.Message{
		Topic:   msg.Topic,
		Key:     []byte(msg.Key),
		Value:   msg.Body,
		Headers: toKafkaHeaders(msg.Headers),
	}
	return p.writer.WriteMessages(ctx, km)
}

func (p *kafkaProducer) Close() error {
	return p.writer.Close()
}

func newKafkaProducer(cfg *mqConf.Kafka, traceSvc trace.ITrace) *kafkaProducer {
	return &kafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Balancer:     &keyAwareBalancer{rr: &kafka.RoundRobin{}},
			RequiredAcks: kafka.RequireAll,
		},
		traceSvc: traceSvc,
	}
}
