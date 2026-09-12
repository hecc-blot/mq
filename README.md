# hecc-blot-mq

面向接口的消息队列组件：统一的生产 / 消费抽象，适配 Kafka / NSQ / RocketMQ，可选延迟投递与顺序消费，自动接入链路追踪。

## 安装

```bash
go get github.com/hecc-blot/mq
```

## 接口定义

```go
import (
    "context"
    mqContract "github.com/hecc-blot/mq/contract"
)

// 生产
type IProducer interface {
    Send(ctx context.Context, msg *Message) error
    Close() error
}

// 消费
type IConsumer interface {
    Subscribe(ctx context.Context, topic, group string, handler Handler, opts ...SubscribeOption) error
    Close() error
}

type Handler func(ctx context.Context, msg *Message) error

// 工厂
type IMqFactory interface {
    Producer() IProducer
    Consumer() IConsumer
}

// 消费者注册器
type IConsume interface {
    Consume(ctx context.Context, msg *Message) error
}

type IConsumerHandle interface {
    Register(topic, group string, consumer IConsume, opts ...SubscribeOption) IConsumerHandle
    Start(ctx context.Context) error
    Shutdown() error
}
```

## 初始化

```go
import (
    mq "github.com/hecc-blot/mq/service"
)

mqFactory, clearUp, err := mq.NewMqFactory(&config.Mq, logSvc, traceSvc)
if err != nil {
    panic(err)
}
defer clearUp()
```

`logSvc` 为 `log.ILog`（core），`traceSvc` 为 `trace.ITrace`（trace），均可传 nil。

## 生产消息

```go
producer := mqFactory.Producer()

err := producer.Send(ctx, &mqContract.Message{
    Topic: "order-created",
    Key:   "order-123",            // 分区键，保证同 Key 有序（Kafka/RocketMQ）
    Body:  []byte(`{"order_id":"123"}`),
})
```

## 消费者注册（推荐）

业务消费者实现 `IConsume`，用 `inject:""` 声明所需依赖（规则与框架的 `IApi` 完全一致），
由注册器统一注册、启动与优雅停止，依赖在**注册期**自动注入。

```go
import (
    "context"
    dbContract "github.com/hecc-blot/db/contract"
    logContract "github.com/hecc-blot/core/contract/log"
)

// 1. 定义消费者：注入字段在前，方法签名与 IApi.Call 同构
type OrderCreatedConsumer struct {
    Db     dbContract.IDb   `inject:""`
    LogSvc logContract.ILog `inject:""`
}

func (c *OrderCreatedConsumer) Consume(ctx context.Context, msg *mqContract.Message) error {
    c.LogSvc.Info(ctx, "订单创建", "body", string(msg.Body))
    return nil // 返回 nil 确认，返回 error 重投
}

// 2. 组装期：注册 → 启动 → defer 停止
//    必须在 container.Set 完所有依赖之后
consumerHandle := mqService.NewConsumerHandle(mqFactory.Consumer(), container, logSvc)
consumerHandle.
    Register("order-created", "order-service", &OrderCreatedConsumer{}).
    Register("order-paid", "order-service", &OrderPaidConsumer{}, mqContract.WithConcurrency(4))

if err := consumerHandle.Start(context.Background()); err != nil {
    panic(err)
}
// 在 defer mqCleanup() 之后注册，LIFO 保证先停消费者、再关连接
defer consumerHandle.Shutdown()
```

约定：

- **注入字段必须放在最前面**。`ioc.Inject` 遍历时遇到第一个没有 `inject` tag 的具名字段即停止，其后的依赖字段会被静默跳过。
- **一个注册对应一个长期实例**。消息是参数传入的，结构体内没有每条消息的状态，故不做请求级隔离。**消费并发 > 1 时 `Consume` 会被并发调用，实现方需自行保证并发安全。**
- **topic + group 必须唯一**。topic/group 为空、consumer 为 nil、同一 topic+group 重复注册、`Start` 之后再注册 —— 都会 panic（启动期编程错误）。
- **`Start` 部分失败会整体关闭**。`IConsumer.Close` 只能整体关闭、无法只回退已成功的订阅，因此失败后注册器不可再用。
- **`Shutdown` 幂等**，可重复调用。

## 直接订阅（底层原语）

不需要依赖注入、或要做运行时动态订阅时，直接用 `IConsumer`：

```go
consumer := mqFactory.Consumer()

err := consumer.Subscribe(ctx, "order-created", "order-service",
    func(ctx context.Context, msg *mqContract.Message) error {
        return handle(msg.Body) // 返回 nil 确认，返回 error 重投
    },
    mqContract.WithConcurrency(4), // 并发数
)
```

## 消息结算

handler 返回值决定结算方向，也可显式调用 `Ack` / `Nack` 精细控制（显式结算优先于返回值）：

| 行为 | 方式 |
|------|------|
| 确认消费 | `return nil` 或 `msg.Ack(ctx)` |
| 失败立即重投 | `return err` 或 `msg.Nack(ctx, err, true)` |
| 失败退避 / 进死信 | `msg.Nack(ctx, err, false)` |

## 可选能力（类型断言）

不同后端能力不同，通过类型断言启用，断言失败表示该后端不支持：

### 延迟投递 `IDelayedProducer`

```go
if p, ok := mqFactory.Producer().(mqContract.IDelayedProducer); ok {
    _ = p.SendDelay(ctx, msg, time.Now().Add(30*time.Second))
}
```

### 顺序消费 `IOrderedConsumer`

```go
if c, ok := mqFactory.Consumer().(mqContract.IOrderedConsumer); ok {
    _ = c.SubscribeOrdered(ctx, topic, group, handler)
}
```

## 后端能力矩阵

| 能力 | Kafka | NSQ | RocketMQ |
|------|-------|-----|----------|
| 生产 | ✅ | ✅ | ✅ |
| 消费 | ✅ | ✅ | ✅ |
| 延迟投递 | ❌ | ✅ 任意时长 | ✅ 预定义等级（1s~2h） |
| 顺序消费 | ✅ 分区内有序 | ❌ | ✅ 队列串行 |
| 链路追踪传播 | ✅ Headers | ❌ 无消息头 | ✅ Properties |
| `WithConcurrency` | ✅ | ✅ 映射为 MaxInFlight | ❌ 由队列驱动，选项被忽略 |

## 配置

```yaml
mq:
  driver: kafka                    # kafka / nsq / rocketmq，默认 kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
  nsq:
    nsqd: ["127.0.0.1:4150"]       # 生产端必填
    nsqlookupd: ["127.0.0.1:4161"] # 可选，服务发现
  rocketmq:
    name_server: ["127.0.0.1:9876"]
    producer_group: mq-producer    # 默认 mq-producer
    access_key: ""                 # ACL 认证，空则不启用
    secret_key: ""
  consumer:
    concurrency: 1                 # 默认消费并发数，默认 1
```

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `driver` | string | 后端类型：`kafka` / `nsq` / `rocketmq`，默认 `kafka` |
| `kafka.brokers` | []string | Kafka broker 地址列表 |
| `nsq.nsqd` | []string | nsqd tcp 地址列表（生产端必填） |
| `nsq.nsqlookupd` | []string | nsqlookupd http 地址列表，服务发现 |
| `rocketmq.name_server` | []string | NameServer 地址列表 |
| `rocketmq.producer_group` | string | 生产者组名，默认 `mq-producer` |
| `rocketmq.access_key` / `secret_key` | string | ACL 认证，空则不启用 |
| `consumer.concurrency` | int | 默认消费并发数，默认 `1` |

## 测试与 mock

业务单测中 mock 掉 MQ，见 `mocks/`：

```go
import "github.com/hecc-blot/mq/mocks"

mockFactory := &mocks.MockMqFactory{}
mockProducer := &mocks.MockProducer{}
mockConsumer := &mocks.MockConsumer{}
mockConsumerHandle := &mocks.MockConsumerHandle{} // 记录 Registrations，便于断言注册行为
```

## 相关模块

| 模块 | 说明 |
|------|------|
| [core](https://github.com/hecc-blot/core) | `log.ILog`（日志契约）、`ioc.IContainer`（依赖注入） |
| [trace](https://github.com/hecc-blot/trace) | `trace.ITrace` 链路追踪传播 |
