# hecc-blot-mq

消息队列模块：统一的生产 / 消费抽象，适配 Kafka / NSQ / RocketMQ，可选延迟投递与顺序消费，自动接入链路追踪。

## 安装

```bash
go get github.com/hecc-blot/mq
```

## 快速开始

```go
import (
	"context"

	logContract "github.com/hecc-blot/core/contract/log"
	ioc "github.com/hecc-blot/core/service/ioc"
	mqContract "github.com/hecc-blot/mq/contract"
	mq "github.com/hecc-blot/mq/service"
	traceContract "github.com/hecc-blot/trace/contract"
)

// 业务消费者：依赖字段在前，其余放在后面
type OrderCreatedConsumer struct {
	LogSvc logContract.ILog `inject:""`
}

func (c *OrderCreatedConsumer) Consume(ctx context.Context, msg *mqContract.Message) error {
	c.LogSvc.Info(ctx, "consume", "topic", msg.Topic)
	return nil // 返回 nil 确认，返回 error 重投
}

func main() {
	// 1. 构造（返回 工厂 / 清理函数 / 错误）；logSvc 与 traceSvc 均可传 nil
	mqFactory, clearUp, err := mq.NewMqFactory(&config.Mq, logSvc, traceSvc)
	if err != nil {
		panic(err)
	}
	defer clearUp() // 一定非 nil，可直接 defer

	// 2. 注册到容器（须早于路由注册）
	container := ioc.New()
	container.Set(new(logContract.ILog), logSvc)
	container.Set(new(mqContract.IMqFactory), mqFactory)

	// 3. 注册消费者 → 启动 → defer 停止
	//    defer 是 LIFO：先停消费者，再走上面的 clearUp 关连接，顺序正确
	consumerHandle := mq.NewConsumerHandle(mqFactory.Consumer(), container, logSvc)
	consumerHandle.Register("order-created", "order-service", &OrderCreatedConsumer{})
	if err := consumerHandle.Start(context.Background()); err != nil {
		panic(err)
	}
	defer consumerHandle.Shutdown()

	// 4. 生产
	_ = mqFactory.Producer().Send(context.Background(), &mqContract.Message{
		Topic: "order-created",
		Body:  []byte(`{"order_id":"123"}`),
	})
}
```

完整的应用组装顺序（日志 → trace → mq → 容器 → 路由）见[组织主页](https://github.com/hecc-blot)。

## 接口定义

```go
import mqContract "github.com/hecc-blot/mq/contract"

// 生产
type IProducer interface {
	Send(ctx context.Context, msg *Message) error // NSQ 后端内部为异步投递
	Close() error
}

// 可选：延迟投递（NSQ / RocketMQ 实现，Kafka 不实现）
type IDelayedProducer interface {
	SendDelay(ctx context.Context, msg *Message, at time.Time) error
}

// 底层消费原语
type IConsumer interface {
	// Subscribe 订阅主题。group 为消费者组/频道。
	// handler 返回 nil 表示消费成功，返回 error 表示失败（默认立即重投）；
	// 也可在 handler 内调用 msg.Ack/Nack 精细控制结算，显式结算优先于返回值。
	Subscribe(ctx context.Context, topic, group string, handler Handler, opts ...SubscribeOption) error
	Close() error
}

// 可选：顺序消费（Kafka 分区内 / RocketMQ 队列内，NSQ 不实现）
type IOrderedConsumer interface {
	SubscribeOrdered(ctx context.Context, topic, group string, handler Handler) error
}

type Handler func(ctx context.Context, msg *Message) error

// 工厂
type IMqFactory interface {
	Producer() IProducer
	Consumer() IConsumer
}

// 消费者注册器（推荐用法）
type IConsume interface {
	Consume(ctx context.Context, msg *Message) error
}

type IConsumerHandle interface {
	Register(topic, group string, consumer IConsume, opts ...SubscribeOption) IConsumerHandle
	Start(ctx context.Context) error
	Shutdown() error
}
```

## 使用

### 消费者注册（推荐）

业务消费者实现 `IConsume`，用 `inject:""` 声明所需依赖（规则与 HTTP 的 `IApi` 一致：**注入字段必须放在最前面**），由注册器统一注册、启动与优雅停止，依赖在**注册期**自动注入。

```go
import (
	"context"

	dbContract "github.com/hecc-blot/db/contract"
	logContract "github.com/hecc-blot/core/contract/log"
	mqContract "github.com/hecc-blot/mq/contract"
)

type OrderCreatedConsumer struct {
	Db     dbContract.IDb   `inject:""`
	LogSvc logContract.ILog `inject:""`
}

func (c *OrderCreatedConsumer) Consume(ctx context.Context, msg *mqContract.Message) error {
	c.LogSvc.Info(ctx, "订单创建", "body", string(msg.Body))
	return nil
}

type OrderPaidConsumer struct {
	LogSvc logContract.ILog `inject:""`
}

func (c *OrderPaidConsumer) Consume(ctx context.Context, msg *mqContract.Message) error {
	return nil
}

// 组装期（必须在 container.Set 完所有依赖之后）
consumerHandle := mq.NewConsumerHandle(mqFactory.Consumer(), container, logSvc)
consumerHandle.
	Register("order-created", "order-service", &OrderCreatedConsumer{}).
	Register("order-paid", "order-service", &OrderPaidConsumer{}, mqContract.WithConcurrency(4))

if err := consumerHandle.Start(context.Background()); err != nil {
	panic(err)
}
defer consumerHandle.Shutdown()
```

约定：

- **注入字段必须放在最前面**。`ioc.Inject` 遍历时遇到第一个没有 `inject` tag 的具名字段即停止，其后的依赖字段会被静默跳过。
- **一个注册对应一个长期实例**。消息是参数传入的，结构体内没有每条消息的状态，故不做请求级隔离。**消费并发 > 1 时 `Consume` 会被并发调用，实现方需自行保证并发安全。**
- **topic + group 必须唯一**。topic/group 为空、consumer 为 nil、同一 topic+group 重复注册、`Start` 之后再注册 —— 都会 panic。
- **`Start` 部分失败会整体关闭**。`IConsumer.Close` 只能整体关闭、无法只回退已成功的订阅，因此失败后注册器不可再用。
- **`Shutdown` 幂等**，可重复调用。
- **`NewConsumerHandle` 的 `consumer` 或 `container` 为 nil 都会 panic**。

### 直接订阅（底层原语）

不需要依赖注入、或要做运行时动态订阅时，直接用 `IConsumer`：

```go
consumer := mqFactory.Consumer()

err := consumer.Subscribe(ctx, "order-created", "order-service",
	func(ctx context.Context, msg *mqContract.Message) error {
		return handle(msg.Body)
	},
	mqContract.WithConcurrency(4),
)
```

### 生产消息

```go
err := mqFactory.Producer().Send(ctx, &mqContract.Message{
	Topic: "order-created",
	Key:   "order-123", // 分区键，保证同 Key 有序（Kafka/RocketMQ；NSQ 忽略）
	Body:  []byte(`{"order_id":"123"}`),
})
```

延迟投递需要类型断言：

```go
if p, ok := mqFactory.Producer().(mqContract.IDelayedProducer); ok {
	_ = p.SendDelay(ctx, msg, time.Now().Add(30*time.Second))
}
```

### 消息结算

handler 返回值决定结算方向，也可显式调用 `Ack` / `Nack` 精细控制：

| 行为 | 方式 |
|------|------|
| 确认消费 | `return nil` 或 `msg.Ack(ctx)` |
| 失败立即重投 | `return err` 或 `msg.Nack(ctx, err, true)` |
| 失败退避 / 进死信 | `msg.Nack(ctx, err, false)` |

- **显式结算优先于返回值**：`Ack` / `Nack` 是 CAS 幂等的，首次结算生效、重复调用无副作用。
- **`ctx` 参数当前被忽略**，为后续异步结算预留。

### 顺序消费

```go
if c, ok := mqFactory.Consumer().(mqContract.IOrderedConsumer); ok {
	_ = c.SubscribeOrdered(ctx, topic, group, handler)
}
```

### 链路追踪

生产端在 `Send` 时把当前 span 注入消息载体，消费端在 handler 执行前 Extract 出来，链路自动串起：

| 后端 | 载体 |
|------|------|
| Kafka | `Message.Headers` |
| RocketMQ | 消息 Properties |
| NSQ | ❌ 无消息头，消费端链路断点 |

前提：生产端调用 `Send` 时传入的 `ctx` 里要带 span —— 也就是 HTTP 请求路径上必须注册了 trace 中间件。

## 配置

```yaml
mq:
  driver: kafka                    # kafka / nsq / rocketmq，默认 kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
    start_offset: last             # last（最新，默认）/ first（最早），仅新消费者组首次订阅生效
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

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `driver` | string | `kafka` | 后端类型 |
| `kafka.brokers` | []string | — | Kafka broker 地址列表（必填） |
| `kafka.start_offset` | string | `last` | 新消费者组首次订阅的起始位置：`last`（只消费新消息）/ `first`（回放历史）；仅在该 group 无已提交 offset 时生效 |
| `nsq.nsqd` | []string | — | nsqd tcp 地址列表（生产端必填） |
| `nsq.nsqlookupd` | []string | — | nsqlookupd http 地址列表 |
| `rocketmq.name_server` | []string | — | NameServer 地址列表（必填） |
| `rocketmq.producer_group` | string | `mq-producer` | 生产者组名 |
| `rocketmq.access_key` / `secret_key` | string | 空 | ACL 认证，空则不启用 |
| `consumer.concurrency` | int | 1 | 默认消费并发数，可被 `WithConcurrency` 覆盖 |

缺少对应后端的必填项时 `NewMqFactory` 返回 error；`driver` 不支持、`kafka.start_offset` 非法时同样返回 error。

> `start_offset` 只在**新消费者组首次订阅**（该 group 在分区上没有已提交 offset）时生效。已有 committed offset 的组以其为准——改配置不会重置进度，需要换 group 名或用 `kafka-consumer-groups --reset-offsets`。
>
> 默认 `last` 与 Kafka `auto.offset.reset=latest`、RocketMQ `CONSUME_FROM_LAST_OFFSET`、NSQ 新建 channel 的行为一致，避免 group 名写错 / 环境重建 / offset 过期时整 topic 重放。需要补历史数据时，另起一个**独立 group** 配 `first` 做一次性回放，不要动主 group。

## 后端能力矩阵

| 能力 | Kafka | NSQ | RocketMQ |
|------|-------|-----|----------|
| 生产 / 消费 | ✅ | ✅ | ✅ |
| 延迟投递 | ❌ | ✅ 任意时长（秒级精度） | ✅ 预定义等级（1s~2h） |
| 顺序消费 | ✅ 分区内有序 | ❌ | ✅ 队列串行 |
| 链路追踪传播 | ✅ Headers | ❌ 无消息头 | ✅ Properties |
| `WithConcurrency` | ✅ | ✅ 映射为 MaxInFlight | ❌ 由队列驱动，选项被忽略 |

## 语义与约定

- **`NewMqFactory` 的 `cfg` 不能为 nil**（返回 error）；`logSvc` / `traceSvc` 可以为 nil。
- **`clearUp()` 先关 producer 再关 consumer**，错误被忽略（不返回）；失败路径返回的也是非 nil 的空函数，可以无条件 `defer`。
- **`Shutdown` / `Close` 的顺序**：先 `Shutdown()` 消费者再 `clearUp()`，`defer` 的 LIFO 天然满足。
- **NSQ 会丢弃 `requeue` 标志**：统一走 go-nsq 的原生退避，`Nack(..., false)` 与 `Nack(..., true)` 在 NSQ 上行为一致。
- **消费并发为 0 时 Kafka 不会消费**：`Subscribe` 里 `n <= 0` 会导致一个 reader 都不建、静默返回 nil。经工厂构造不会出现（`Normalize` 兜底为 1），直接构造 consumer 时注意。
- **三个后端的 ctx 语义不一致**：Kafka 用 ctx 驱动拉取循环，NSQ 把 ctx 透传给 handler，RocketMQ 的 handler ctx 由 trace 重建。`Shutdown()` 才是统一的停止手段。

## 测试与 mock

业务单测中 mock 掉 MQ，见 `mocks/`：

```go
import "github.com/hecc-blot/mq/mocks"

mockFactory := &mocks.MockMqFactory{}
mockProducer := &mocks.MockProducer{}   // Sent 记录发出的消息
mockConsumer := &mocks.MockConsumer{}   // Subscriptions 记录订阅（含 Opts）
mockHandle := &mocks.MockConsumerHandle{} // Registrations 记录注册项
```

## 相关模块

| 模块 | 说明 |
|------|------|
| [core](https://github.com/hecc-blot/core) | `log.ILog` 日志契约、`ioc.IContainer` 依赖注入 |
| [trace](https://github.com/hecc-blot/trace) | `trace.ITrace` 链路追踪传播 |
