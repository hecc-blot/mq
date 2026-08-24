package mq

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFinalizeAutoAck(t *testing.T) {
	m := &Message{}
	acked, requeue := m.Finalize(nil)
	assert.True(t, acked)
	assert.False(t, requeue)
}

func TestFinalizeAutoNack(t *testing.T) {
	m := &Message{}
	acked, requeue := m.Finalize(errors.New("boom"))
	assert.False(t, acked)
	assert.True(t, requeue) // 自动 Nack 默认 requeue=true
}

func TestExplicitNackWinsOverReturnNil(t *testing.T) {
	m := &Message{}
	assert.NoError(t, m.Nack(context.Background(), errors.New("x"), false))
	acked, requeue := m.Finalize(nil) // 即使返回 nil，显式 Nack 优先
	assert.False(t, acked)
	assert.False(t, requeue)
}

func TestExplicitAckWinsOverReturnError(t *testing.T) {
	m := &Message{}
	assert.NoError(t, m.Ack(context.Background()))
	acked, _ := m.Finalize(errors.New("boom")) // 即使返回 err，显式 Ack 优先
	assert.True(t, acked)
}

func TestSettleIdempotent(t *testing.T) {
	m := &Message{}
	assert.NoError(t, m.Ack(context.Background()))
	assert.NoError(t, m.Ack(context.Background())) // 幂等
	acked, _ := m.Finalize(nil)
	assert.True(t, acked)
}

func TestApplySubscribeOptions(t *testing.T) {
	cfg := ApplySubscribeOptions(WithConcurrency(5))
	assert.Equal(t, 5, cfg.Concurrency)

	assert.Equal(t, 0, ApplySubscribeOptions().Concurrency)
}
