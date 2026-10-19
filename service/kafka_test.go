package service

import (
	"testing"

	mqConf "github.com/hecc-blot/mq/config"
	mqEnum "github.com/hecc-blot/mq/enum/mq"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestKafkaHeadersRoundTrip(t *testing.T) {
	in := map[string]string{"traceparent": "00-abc", "k": "v"}
	out := fromKafkaHeaders(toKafkaHeaders(in))
	assert.Equal(t, in, out)
}

func TestKafkaHeadersEmpty(t *testing.T) {
	assert.Nil(t, toKafkaHeaders(nil))
	assert.Nil(t, fromKafkaHeaders(nil))
}

func TestKafkaStartOffsetMapping(t *testing.T) {
	assert.Equal(t, kafka.FirstOffset, kafkaStartOffset(mqEnum.FirstOffset))
	assert.Equal(t, kafka.LastOffset, kafkaStartOffset(mqEnum.LastOffset))
	// 空值兜底为模块默认 last，与 Normalize 的默认值一致。
	assert.Equal(t, kafka.LastOffset, kafkaStartOffset(""))
}

func TestNewKafkaConsumerStartOffset(t *testing.T) {
	c := newKafkaConsumer(
		&mqConf.Kafka{StartOffset: mqEnum.LastOffset},
		&mqConf.Consumer{Concurrency: 1},
		nil, nil,
	)
	assert.Equal(t, kafka.LastOffset, c.startOffset)
}
