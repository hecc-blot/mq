package service

import (
	"testing"

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
