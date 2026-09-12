package mq

import "context"

// IConsume 业务消费者单元。一个实现对应一个 topic + group 的消费逻辑。
//
// 依赖字段用 `inject:""` 声明并放在结构体最前面，规则与框架的 IApi 完全一致
// （core/service/ioc 的 Inject 遇到第一个没有 inject tag 的具名字段即停止注入）：
//
//	type OrderCreatedConsumer struct {
//		Db     dbContract.IDb   `inject:""`   // 注入字段在前
//		LogSvc logContract.ILog `inject:""`
//	}
//
//	func (c *OrderCreatedConsumer) Consume(ctx context.Context, msg *Message) error { ... }
//
// 注入在 Register 时一次完成，之后长期复用同一实例（消息是参数传入的，结构体内
// 没有每条消息的状态，故不做请求级隔离）。因此消费并发 > 1 时 Consume 会被并发
// 调用，实现方需自行保证并发安全。
type IConsume interface {
	// Consume 处理一条消息。返回 nil 表示消费成功，返回 error 表示失败（默认立即重投）；
	// 也可在方法内调用 msg.Ack/Nack 精细控制结算，显式结算优先于返回值。
	Consume(ctx context.Context, msg *Message) error
}

// IConsumerHandle 消费者注册器：统一注册、启动、优雅停止，并自动完成依赖注入。
//
// 典型用法（组装期，且必须在 IOC 容器 Set 完所需依赖之后）：
//
//	handle := mqService.NewConsumerHandle(mqFactory.Consumer(), container, logSvc)
//	handle.
//		Register("order-created", "order-service", &OrderCreatedConsumer{}).
//		Register("order-paid", "order-service", &OrderPaidConsumer{}, mqContract.WithConcurrency(4))
//	if err := handle.Start(context.Background()); err != nil {
//		panic(err)
//	}
//	defer handle.Shutdown()
type IConsumerHandle interface {
	// Register 注册一个消费者。topic / group 语义与 IConsumer.Subscribe 一致，
	// opts 原样透传（如 WithConcurrency）。依赖在此刻完成注入。
	//
	// topic / group 为空、consumer 为 nil、同一 topic+group 重复注册、
	// 或 Start 之后再注册，都会 panic —— 这些都是启动期的编程错误。
	Register(topic, group string, consumer IConsume, opts ...SubscribeOption) IConsumerHandle

	// Start 启动所有已注册消费者。任一订阅失败会关闭底层 consumer 并返回该错误
	// （IConsumer.Close 只能整体关闭，无法只回退已成功的部分），此后注册器不可再用。
	// 重复调用返回错误。
	Start(ctx context.Context) error

	// Shutdown 优雅停止：停止接收新消息，等待在途消息处理完成。幂等，可重复调用。
	Shutdown() error
}
