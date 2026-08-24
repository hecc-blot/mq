package service

import (
	"testing"
	"time"

	mqContract "github.com/hecc-blot/mq/contract"
	"github.com/stretchr/testify/assert"
)

// 编译期断言可选能力接口的落地情况，防止适配器能力退化。
var (
	_ mqContract.IDelayedProducer = (*nsqProducer)(nil)
	_ mqContract.IDelayedProducer = (*rocketmqProducer)(nil)
	_ mqContract.IOrderedConsumer = (*kafkaConsumer)(nil)
	_ mqContract.IOrderedConsumer = (*rocketmqConsumer)(nil)
)

func TestRocketmqDelayLevel(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want int
	}{
		{0, 0},
		{-time.Second, 0},
		{time.Millisecond, 1},
		{1 * time.Second, 1},
		{6 * time.Second, 3}, // 向上取整到 10s
		{10 * time.Second, 3},
		{31 * time.Second, 5}, // 向上取整到 1m
		{90 * time.Second, 6}, // 向上取整到 2m
		{2 * time.Hour, 18},
		{3 * time.Hour, 18}, // 超 2h 取最大等级
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, rocketmqDelayLevel(tc.d), "duration=%v", tc.d)
	}
}
