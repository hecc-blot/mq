package service

import (
	"testing"

	mqConf "github.com/hecc-blot/mq/config"
	mqEnum "github.com/hecc-blot/mq/enum/mq"
	"github.com/stretchr/testify/assert"
)

func TestNewMqFactoryNilConfig(t *testing.T) {
	_, _, err := NewMqFactory(nil, nil, nil)
	assert.Error(t, err)
}

func TestNewMqFactoryKafkaWithoutBrokers(t *testing.T) {
	cfg := mqConf.Normalize(mqConf.Config{})
	_, _, err := NewMqFactory(&cfg, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "brokers")
}

func TestNewMqFactoryUnsupportedDriver(t *testing.T) {
	cfg := mqConf.Config{Driver: mqEnum.Value("unknown")}
	_, _, err := NewMqFactory(&cfg, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "不支持")
}

func TestNewMqFactoryNsqWithoutNsqd(t *testing.T) {
	cfg := mqConf.Config{Driver: mqEnum.Nsq}
	_, _, err := NewMqFactory(&cfg, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nsqd")
}

func TestNewMqFactoryRocketMQWithoutNameServer(t *testing.T) {
	cfg := mqConf.Config{Driver: mqEnum.RocketMQ}
	_, _, err := NewMqFactory(&cfg, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name_server")
}
