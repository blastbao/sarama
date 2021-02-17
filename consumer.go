package sarama

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rcrowley/go-metrics"
)

// ConsumerMessage encapsulates a Kafka message returned by the consumer.
type ConsumerMessage struct {
	// 消息头
	Headers        []*RecordHeader // only set if kafka is version 0.11+
	// 时间戳
	Timestamp      time.Time       // only set if kafka is version 0.10+, inner message timestamp
	// 块时间戳
	BlockTimestamp time.Time       // only set if kafka is version 0.10+, outer (compressed) block timestamp

	// 消息内容
	Key, Value []byte

	// 基础信息
	Topic      string
	Partition  int32
	Offset     int64
}

// ConsumerError is what is provided to the user when an error occurs.
// It wraps an error and includes the topic and partition.
type ConsumerError struct {
	Topic     string
	Partition int32
	Err       error
}

func (ce ConsumerError) Error() string {
	return fmt.Sprintf("kafka: error while consuming %s/%d: %s", ce.Topic, ce.Partition, ce.Err)
}

func (ce ConsumerError) Unwrap() error {
	return ce.Err
}

// ConsumerErrors is a type that wraps a batch of errors and implements the Error interface.
// It can be returned from the PartitionConsumer's Close methods to avoid the need to manually drain errors
// when stopping.
type ConsumerErrors []*ConsumerError

func (ce ConsumerErrors) Error() string {
	return fmt.Sprintf("kafka: %d errors while consuming", len(ce))
}

// Consumer manages PartitionConsumers which process Kafka messages from brokers. You MUST call Close()
// on a consumer to avoid leaks, it will not be garbage-collected automatically when it passes out of scope.
//
// Consumer 管理 PartitionConsumers ，它处理来自 broker 的 Kafka 消息。
// 使用者必须在结束时调用 Close() 以避免泄漏，当它超出作用域时，不会自动进行垃圾收集。
//
type Consumer interface {

	// Topics returns the set of available topics as retrieved from the cluster
	// metadata. This method is the same as Client.Topics(), and is provided for
	// convenience.
	//
	// Topics() 从集群元数据中获取可用的 Topic 集合。
	// 此方法与 Client.Topics() 相同，是为了方便而提供的。
	Topics() ([]string, error)

	// Partitions returns the sorted list of all partition IDs for the given topic.
	// This method is the same as Client.Partitions(), and is provided for convenience.
	//
	// Partitions() 返回给定 Topic 的所有分区 id 的有序列表。
	// 此方法与 Client.Partitions() 相同，是为了方便而提供的。
	Partitions(topic string) ([]int32, error)

	// ConsumePartition creates a PartitionConsumer on the given topic/partition with
	// the given offset. It will return an error if this Consumer is already consuming
	// on the given topic/partition. Offset can be a literal offset, or OffsetNewest
	// or OffsetOldest
	//
	// ConsumePartition() 在给定的 topic/partition 上使用给定的偏移量创建一个 PartitionConsumer 。
	// 如果这个消费者已经在给定的主题/分区上消费，它将返回一个错误。Offset可以是一个文字偏移量，或者是offsetlatest或者OffsetOldest
	//
	ConsumePartition(topic string, partition int32, offset int64) (PartitionConsumer, error)

	// HighWaterMarks returns the current high water marks for each topic and partition.
	// Consistency between partitions is not guaranteed since high water marks are updated separately.
	HighWaterMarks() map[string]map[int32]int64

	// Close shuts down the consumer. It must be called after all child
	// PartitionConsumers have already been closed.
	Close() error
}


// 是 Consumer 接口的实现类，包含消费者的所有核心逻辑。
//
type consumer struct {
	conf            *Config									// 消费者配置
	children        map[string]map[int32]*partitionConsumer // 每个 topic/partition 对应一个 partitionConsumer
	brokerConsumers map[*Broker]*brokerConsumer				// 每个 broker 对应一个 brokerConsumer
	client          Client									// [核心]
	lock            sync.Mutex
}

// NewConsumer creates a new consumer using the given broker addresses and configuration.
func NewConsumer(addrs []string, config *Config) (Consumer, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}
	return newConsumer(client)
}

// NewConsumerFromClient creates a new consumer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
func NewConsumerFromClient(client Client) (Consumer, error) {
	// For clients passed in by the client, ensure we don't call Close() on it.
	cli := &nopCloserClient{client}
	return newConsumer(cli)
}

func newConsumer(client Client) (Consumer, error) {

	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}

	c := &consumer{
		client:          client,
		conf:            client.Config(),
		children:        make(map[string]map[int32]*partitionConsumer),
		brokerConsumers: make(map[*Broker]*brokerConsumer),
	}

	return c, nil
}

func (c *consumer) Close() error {
	return c.client.Close()
}

func (c *consumer) Topics() ([]string, error) {
	return c.client.Topics()
}

// 根据 topic 获取 partitionIds
func (c *consumer) Partitions(topic string) ([]int32, error) {
	return c.client.Partitions(topic)
}


// 消费 topic/partition/offset 的数据:
//
// 	构造 partitionConsumer 对象
// 	获取待消费的 offset ，保存到 partitionConsumer.offset 上
// 	把 partitionConsumer 对象保存到 consumer.children[topic][partition] 上
//  启动 dispatcher 协程: 监听 brokerConsumer 的异常
//  启动 responseFeeder 协程: 监听 brokerConsumer 返回的数据
//	获取 topic/partition 的 leader broker 对象 (顺序调整)
//  获取 leader broker 对应的 brokerConsumer ，若不存在则创建
//  把 partitionConsumer 对象注册到 brokerConsumer ，接收 topic/partition 消息的回调
func (c *consumer) ConsumePartition(topic string, partition int32, offset int64) (PartitionConsumer, error) {

	//
	child := &partitionConsumer{
		consumer:  c,
		conf:      c.conf,
		topic:     topic,
		partition: partition,
		messages:  make(chan *ConsumerMessage, c.conf.ChannelBufferSize),
		errors:    make(chan *ConsumerError, c.conf.ChannelBufferSize),
		feeder:    make(chan *FetchResponse, 1),
		trigger:   make(chan none, 1),
		dying:     make(chan none),
		fetchSize: c.conf.Consumer.Fetch.Default,
	}

	// 设置 child 的 offset 变量
	if err := child.chooseStartingOffset(offset); err != nil {
		return nil, err
	}

	var leader *Broker
	var err error

	// 获取 leader broker ，每个 topic/partition 对应一个确定的 leader broker
	if leader, err = c.client.Leader(child.topic, child.partition); err != nil {
		return nil, err
	}

	// 把 child 保存到 topic/partition 的订阅者列表(map)中
	if err := c.addChild(child); err != nil {
		return nil, err
	}

	// 跟踪 broker 的变化，偏元信息性质的控制侧；
	go withRecover(child.dispatcher)
	// 监听 brokerConsumer 返回的数据
	go withRecover(child.responseFeeder)

	// 获取 leader broker 对应的 brokerConsumer
	child.broker = c.refBrokerConsumer(leader)

	// partitionConsumer 通过 brokerConsumer.input 这个 chan 加入到 brokerConsumer 的订阅中；
	child.broker.input <- child

	return child, nil
}

func (c *consumer) HighWaterMarks() map[string]map[int32]int64 {
	c.lock.Lock()
	defer c.lock.Unlock()

	// topic/partition => offset

	hwms := make(map[string]map[int32]int64)
	for topic, p := range c.children {
		hwm := make(map[int32]int64, len(p))
		for partition, pc := range p {
			hwm[partition] = pc.HighWaterMarkOffset()
		}
		hwms[topic] = hwm
	}

	return hwms
}

// 把 child 保存到 topic/partition 的订阅者列表(map)中
func (c *consumer) addChild(child *partitionConsumer) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	topicChildren := c.children[child.topic]
	if topicChildren == nil {
		topicChildren = make(map[int32]*partitionConsumer)
		c.children[child.topic] = topicChildren
	}

	// 一个 consumer 不能重复消费同个 topic/partition
	if topicChildren[child.partition] != nil {
		return ConfigurationError("That topic/partition is already being consumed")
	}

	// 保存 child 到 topic/partition 的订阅者列表中
	topicChildren[child.partition] = child
	return nil
}

// 从 c.children[topic][partition] 中移除 child.partition
func (c *consumer) removeChild(child *partitionConsumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.children[child.topic], child.partition)
}

func (c *consumer) refBrokerConsumer(broker *Broker) *brokerConsumer {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 每个 broker 对应一个 brokerConsumer ，不同 partition/topic 对应一个 leader broker ，通过引用计数维护。
	bc := c.brokerConsumers[broker]
	if bc == nil {
		bc = c.newBrokerConsumer(broker)	// 不存在则创建
		c.brokerConsumers[broker] = bc
	}

	// 引用计数 +1
	bc.refs++

	return bc
}

func (c *consumer) unrefBrokerConsumer(brokerWorker *brokerConsumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 减引用
	brokerWorker.refs--

	// 如果引用计数为 0 则关闭 brokerConsumer 并从 c.brokerConsumers[] 中清理它
	if brokerWorker.refs == 0 {
		close(brokerWorker.input)
		if c.brokerConsumers[brokerWorker.broker] == brokerWorker {
			delete(c.brokerConsumers, brokerWorker.broker)
		}
	}
}

func (c *consumer) abandonBrokerConsumer(brokerWorker *brokerConsumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.brokerConsumers, brokerWorker.broker)
}

// PartitionConsumer

// PartitionConsumer processes Kafka messages from a given topic and partition. You MUST call one of Close() or
// AsyncClose() on a PartitionConsumer to avoid leaks; it will not be garbage-collected automatically when it passes out
// of scope.
//
// The simplest way of using a PartitionConsumer is to loop over its Messages channel using a for/range
// loop. The PartitionConsumer will only stop itself in one case: when the offset being consumed is reported
// as out of range by the brokers. In this case you should decide what you want to do (try a different offset,
// notify a human, etc) and handle it appropriately. For all other error cases, it will just keep retrying.
// By default, it logs these errors to sarama.Logger; if you want to be notified directly of all errors, set
// your config's Consumer.Return.Errors to true and read from the Errors channel, using a select statement
// or a separate goroutine. Check out the Consumer examples to see implementations of these different approaches.
//
// To terminate such a for/range loop while the loop is executing, call AsyncClose. This will kick off the process of
// consumer tear-down & return immediately. Continue to loop, servicing the Messages channel until the teardown process
// AsyncClose initiated closes it (thus terminating the for/range loop). If you've already ceased reading Messages, call
// Close; this will signal the PartitionConsumer's goroutines to begin shutting down (just like AsyncClose), but will
// also drain the Messages channel, harvest all errors & return them once cleanup has completed.
//
//
//
// PartitionConsumer 处理来自给定 topic 和 partition 的 Kafka 消息。
//
// 你必须在 PartitionConsumer 上调用 Close() 或 AsyncClose() 来避免泄露，当它超出作用域时，不会自动进行垃圾收集。
//
// 使用 PartitionConsumer 最简单方法是通过 for/range 在其消息通道上循环消费。
//
// PartitionConsumer 只会在一种情况下停止自己：
// 当待消费的 offset 超出 broker 范围，在这种情况下，您应该决定采取合适的措施(尝试不同的偏移量、告警通知等等)来处理它。
// 对于所有其他错误情况，PartitionConsumer 会继续重试。
//
// 默认情况下，会将这些错误记录到 sarama.Logger ，如果调用者想直接得到所有错误的通知，需要配置 Consumer.Return.Errors 为 true ，
// 并使用 select 语句或单独的 goroutine 从 Errors 管道中读取错误。
//
// 若要终止消费，可以通过调用 AsyncClose / Close 。
//
//
// 除非调用 AsyncClose() 或者 Close() 方法，ConsumePartition() 启动的消费过程会永不停止，遇到错误，就不停重试，
// 如果开启 Consumer.Return.Errors，则可以将收到的错误交给 Errors 这个 channel 允许用户处理。
//
type PartitionConsumer interface {

	// AsyncClose initiates a shutdown of the PartitionConsumer. This method will return immediately, after which you
	// should continue to service the 'Messages' and 'Errors' channels until they are empty. It is required to call this
	// function, or Close before a consumer object passes out of scope, as it will otherwise leak memory. You must call
	// this before calling Close on the underlying client.
	AsyncClose()

	// Close stops the PartitionConsumer from fetching messages. It will initiate a shutdown just like AsyncClose, drain
	// the Messages channel, harvest any errors & return them to the caller. Note that if you are continuing to service
	// the Messages channel when this function is called, you will be competing with Close for messages; consider
	// calling AsyncClose, instead. It is required to call this function (or AsyncClose) before a consumer object passes
	// out of scope, as it will otherwise leak memory. You must call this before calling Close on the underlying client.
	Close() error

	// Messages returns the read channel for the messages that are returned by the broker.
	Messages() <-chan *ConsumerMessage

	// Errors returns a read channel of errors that occurred during consuming, if enabled.
	// By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan *ConsumerError

	// HighWaterMarkOffset returns the high water mark offset of the partition,
	// i.e. the offset that will be used for the next message that will be produced.
	// You can use this to determine how far behind the processing is.
	HighWaterMarkOffset() int64
}

type partitionConsumer struct {

	highWaterMarkOffset int64 // must be at the top of the struct because https://golang.org/pkg/sync/atomic/#pkg-note-BUG

	consumer *consumer				//
	conf     *Config				//
	broker   *brokerConsumer		//
	messages chan *ConsumerMessage	// 消息管道，
	errors   chan *ConsumerError	// 错误管道，出错时写入此管道
	feeder   chan *FetchResponse	//


	// 读写分离级别配置: 首选主节点，大多情况下读操作在主节点，如果主节点不可用，如故障转移，读操作在从节点。
	preferredReadReplica int32

	trigger, dying chan none
	closeOnce      sync.Once
	topic          string		// 订阅的 topic
	partition      int32		// 订阅的 partition
	responseResult error		// 错误
	fetchSize      int32		// 拉取数据量
	offset         int64		// 偏移量
	retries        int32		// 重试次数
}


var errTimedOut = errors.New("timed out feeding messages to the user") // not user-facing


func (child *partitionConsumer) sendError(err error) {

	// 构造错误对象
	cErr := &ConsumerError{
		Topic:     child.topic,		// 主题
		Partition: child.partition,	// 分区
		Err:       err,				// 错误
	}

	// 如果需要返回错误，就写入到 child.errors 管道中，否则仅打印一下
	if child.conf.Consumer.Return.Errors {
		child.errors <- cErr
	} else {
		Logger.Println(cErr)
	}
}

// 计算退让时间间隔，控制读取分区失败后，等待多长时间才能再次尝试(默认为2s)。
func (child *partitionConsumer) computeBackoff() time.Duration {

	// 用来动态计算回退时间。
	if child.conf.Consumer.Retry.BackoffFunc != nil {
		// 增加重试次数
		retries := atomic.AddInt32(&child.retries, 1)
		// 基于重试次数控制回退间隔
		return child.conf.Consumer.Retry.BackoffFunc(int(retries))
	}

	// 读取分区失败后要等待多长时间才能再次尝试(默认为2s)。
	return child.conf.Consumer.Retry.Backoff
}

//
// 如果从 broker 拉取消息出现错误，会触发 child.trigger 管道，执行 broker 的重新选择和加入订阅。
//
func (child *partitionConsumer) dispatcher() {

	// 只要 child.trigger 管道上有通知，意味着 brokerConsumer 上的消费遇到错误，需要重新选择新 broker 并订阅。

	for range child.trigger {

		// 注意，没有 default 意味着 select 会一直阻塞。

		select {
		// 如果 child 正在关闭中，则 close(child.trigger) 触发 for 循环退出。
		case <-child.dying:
			close(child.trigger)
		// 计算退让时间间隔，等待超时到达(默认为2s)。
		case <-time.After(child.computeBackoff()):

			// 如果 broker 非空 ，解除订阅并重置为空
			if child.broker != nil {
				child.consumer.unrefBrokerConsumer(child.broker)
				child.broker = nil
			}

			Logger.Printf("consumer/%s/%d finding new broker\n", child.topic, child.partition)

			// 重新选择一个可用的 broker 并加入订阅，如果出错，则写入 errors 管道通知用户，并写入 trigger 管道触发延迟重试
			if err := child.dispatch(); err != nil {
				child.sendError(err)
				child.trigger <- none{}
			}
		}

	}

	// 至此，意味着 child.trigger 被 close() ，也即 child.dying 被触发，child 已经关闭。

	// 如果 broker 非空 ，解除订阅并重置为空
	if child.broker != nil {
		child.consumer.unrefBrokerConsumer(child.broker)
	}

	// 从 c.children[topic][partition] 中移除 child.partition
	child.consumer.removeChild(child)

	// [重要] 通过 close(child.feeder) 关闭 responseFeeder() 协程
	close(child.feeder)
	// [重要] dispatcher() 协程退出
	// ...
}

func (child *partitionConsumer) preferredBroker() (*Broker, error) {
	// 如果主 broker 有对应的从节点，可以优先使用该从节点
	if child.preferredReadReplica >= 0 {
		// 根据从 broker 节点的 id 获取 broker 对象
		broker, err := child.consumer.client.Broker(child.preferredReadReplica)
		if err == nil {
			return broker, nil
		}
	}

	// if prefered replica cannot be found fallback to leader
	// 如果主 broker 节点无对应从节点，则从新获取 topic/partition 的 leader broker
	return child.consumer.client.Leader(child.topic, child.partition)
}

func (child *partitionConsumer) dispatch() error {
	// 刷新 topic 的元数据
	if err := child.consumer.client.RefreshMetadata(child.topic); err != nil {
		return err
	}

	// 查找 topic/partition 的 broker ，可能是 preferReadReplica broker 或者 leader broker
	broker, err := child.preferredBroker()
	if err != nil {
		return err
	}

	// 获取 broker 对应的 brokerConsumer ，若不存在就新建一个，并保存到 child.broker 。
	child.broker = child.consumer.refBrokerConsumer(broker)

	// 使 partitionConsumer 加入 brokerConsumer 的订阅，当消息到达时会回调通知
	child.broker.input <- child

	return nil
}

func (child *partitionConsumer) chooseStartingOffset(offset int64) error {

	// 获取最新 offset
	newestOffset, err := child.consumer.client.GetOffset(child.topic, child.partition, OffsetNewest)
	if err != nil {
		return err
	}

	// 获取最旧 offset
	oldestOffset, err := child.consumer.client.GetOffset(child.topic, child.partition, OffsetOldest)
	if err != nil {
		return err
	}

	switch {
	case offset == OffsetNewest:
		child.offset = newestOffset
	case offset == OffsetOldest:
		child.offset = oldestOffset
	case offset >= oldestOffset && offset <= newestOffset:
		child.offset = offset
	default:
		// 如果 offset 超过区间，报错 OutOfRange
		return ErrOffsetOutOfRange
	}

	return nil
}

func (child *partitionConsumer) Messages() <-chan *ConsumerMessage {
	return child.messages
}

func (child *partitionConsumer) Errors() <-chan *ConsumerError {
	return child.errors
}

func (child *partitionConsumer) AsyncClose() {
	// this triggers whatever broker owns this child to abandon it and close its trigger channel, which causes
	// the dispatcher to exit its loop, which removes it from the consumer then closes its 'messages' and
	// 'errors' channel (alternatively, if the child is already at the dispatcher for some reason, that will
	// also just close itself)
	child.closeOnce.Do(func() {
		close(child.dying)
	})
}

func (child *partitionConsumer) Close() error {

	child.AsyncClose()

	var consumerErrors ConsumerErrors
	for err := range child.errors {
		consumerErrors = append(consumerErrors, err)
	}

	if len(consumerErrors) > 0 {
		return consumerErrors
	}
	return nil
}

func (child *partitionConsumer) HighWaterMarkOffset() int64 {
	return atomic.LoadInt64(&child.highWaterMarkOffset)
}

// 从 child.feeder 管道中接收 brokerConsumer 从 broker 拉取的消息
func (child *partitionConsumer) responseFeeder() {

	// 全局变量，用于存储从管道中接收的发自 brokerConsumer 的 messages
	var msgs []*ConsumerMessage

	// 配置项 `child.conf.Consumer.MaxProcessingTime` 默认值为 100ms ，
	// 它的意思是如果朝 messages chan 写入超过 100ms 仍未成功，则停止再向 Broker 发送 fetch 请求。
	expiryTicker := time.NewTicker(child.conf.Consumer.MaxProcessingTime)

	firstAttempt := true

feederLoop:

	// 从 child.feeder 管道监听 brokerConsumer 返回的数据；
	//
	// 注意：
	// brokerConsumer 在把数据写入到 partitionConsumer.feeder 后，会阻塞在 child.broker.acks.Wait() 上，
	// 等待这里接收并处理完 msgs 之后，调用 child.broker.acks.Done() 解除阻塞。
	for response := range child.feeder {

		// 解析数据
		msgs, child.responseResult = child.parseResponse(response)
		if child.responseResult == nil {
			atomic.StoreInt32(&child.retries, 0)
		}

		// 遍历接收到的每条消息
		for i, msg := range msgs {

			// 在将 msg 发往 messages 管道前做一些逻辑
			for _, interceptor := range child.conf.Consumer.Interceptors {
				msg.safelyApplyInterceptor(interceptor)
			}

			// 对每一条消息，执行:
			// 1. 将 msg 写入到 child.messages 管道
			//		1. 如果写入成功，更新 `firstAttempt` 变量为 true
			// 2. 监听 child 是否已关闭
			// 3. 监听写入操作是否已经超时
			//		1. 如果本次超时前，有成功写入管道的 msg，即 `firstAttempt` 为 true ，则重置 `firstAttempt` 后 continue 回 for-loop 重试一次，再等待一次超时
			//		2. 如果本次超时前，未有成功写入的 msg 或者离最近一次成功写入已经超过一个超时间隔，即 `firstAttempt` 为 false ，则：
			//			1. 设置错误信息
			//			2. 解除 brokerConsumer 的阻塞等待
			//			3. 再重试将剩余消息写入管道
			//          4. ??? 将 partitionConsumer 重新加入 brokerConsumer 的订阅
			//
		messageSelect:
			select {
			case <-child.dying:				// 如果 child 正在关闭，则回到 for 循环处，等待 child.feeder 被关闭后退出循环
				child.broker.acks.Done()
				continue feederLoop
			case child.messages <- msg: 	// [核心] 将消息写入到 messages 管道，通知调用者，若首次写入成功，更新 `firstAttempt` 变量为 true
				firstAttempt = true
			case <-expiryTicker.C:			// [核心] 如果超过 100ms 仍未将消息成功写入管道

				// 如果 `firstAttempt` 为 false ，意味着首次写入失败，且等待 100ms 后仍未写成功
				if !firstAttempt {
					// 设置超时错误，后面 brokerConsumer 得到错误后会解除订阅关系，停止拉取和投递本 child 的消息。
					child.responseResult = errTimedOut // 用户迟迟没有从 child.messages 管道中读取消息
					// 解除 brokerConsumer 阻塞
					child.broker.acks.Done()

					// 当前处理到第 i 条消息，对剩余的每条消息进行处理，注意，这里是阻塞式的写管道操作
				remainingLoop:
					for _, msg = range msgs[i:] {
						select {
						case child.messages <- msg:
						case <-child.dying:
							break remainingLoop
						}
					}

					// 如果写入消息成功，重新将 partitionConsumer 加入 brokerConsumer 的订阅
					child.broker.input <- child
					continue feederLoop

				// 如果 `firstAttempt` 为 true ，意味着首次写入成功，但是时隔 100ms 仍未有成功写入，
				} else {
					// current message has not been sent, return to select statement
					firstAttempt = false
					goto messageSelect
				}
			}
		}

		// 解除 brokerConsumer 阻塞式等待
		child.broker.acks.Done()
	}

	// 运行至此，意味着 dispatcher 协程已经执行完 close(child.feeder) 并退出。

	// 关闭超时定时器
	expiryTicker.Stop()
	// 关闭消息管道
	close(child.messages)
	// 关闭错误管道
	close(child.errors)
}

func (child *partitionConsumer) parseMessages(msgSet *MessageSet) ([]*ConsumerMessage, error) {

	var messages []*ConsumerMessage

	for _, msgBlock := range msgSet.Messages {
		for _, msg := range msgBlock.Messages() {
			// 消息偏移量
			offset := msg.Offset
			// 消息时间戳
			timestamp := msg.Msg.Timestamp
			if msg.Msg.Version >= 1 {
				baseOffset := msgBlock.Offset - msgBlock.Messages()[len(msgBlock.Messages())-1].Offset
				offset += baseOffset
				if msg.Msg.LogAppendTime {
					timestamp = msgBlock.Msg.Timestamp
				}
			}
			// 忽略低 offset 消息
			if offset < child.offset {
				continue
			}
			// 格式转换
			messages = append(messages, &ConsumerMessage{
				Topic:          child.topic,
				Partition:      child.partition,
				Key:            msg.Msg.Key,
				Value:          msg.Msg.Value,
				Offset:         offset,
				Timestamp:      timestamp,
				BlockTimestamp: msgBlock.Msg.Timestamp,
			})
			// 更新 offset
			child.offset = offset + 1
		}
	}
	if len(messages) == 0 {
		child.offset++
	}
	return messages, nil
}


// 把 RecordBatch 转换成 []*ConsumerMessage
func (child *partitionConsumer) parseRecords(batch *RecordBatch) ([]*ConsumerMessage, error) {

	messages := make([]*ConsumerMessage, 0, len(batch.Records))

	// 遍历返回的记录
	for _, rec := range batch.Records {

		// 计算偏移量（相对偏移）
		offset := batch.FirstOffset + rec.OffsetDelta

		// 忽略低于待拉取偏移量的记录
		if offset < child.offset {
			continue
		}

		// 计算时间戳（相对偏移）
		timestamp := batch.FirstTimestamp.Add(rec.TimestampDelta)
		if batch.LogAppendTime {
			timestamp = batch.MaxTimestamp
		}

		// 把 record 转换为 ConsumerMessage 对象，并保存到 massages 数组中
		messages = append(messages, &ConsumerMessage{
			Topic:     child.topic,			// topic
			Partition: child.partition,		// partition
			Key:       rec.Key,				// key
			Value:     rec.Value,			// value
			Offset:    offset,				// 记录偏移量
			Timestamp: timestamp,			// 记录时间戳
			Headers:   rec.Headers,			// 记录头
		})

		// 更新 child 已拉取数据的偏移量
		child.offset = offset + 1
	}

	if len(messages) == 0 {
		child.offset++
	}

	return messages, nil
}

// 解析 brokerConsumer 返回的 response 数据
//
//	1. 从 response 取出属于 topic/partition 的块(block)数据，若不存在则报 `ErrIncompleteResponse`，若出错则报错
//  2. 获取数据条数 nRecs
//  3. 获取 preferredReadReplica ，当主 broker 不可用时，使用此 broker
//  4.
//
func (child *partitionConsumer) parseResponse(response *FetchResponse) ([]*ConsumerMessage, error) {

	var (
		metricRegistry          = child.conf.MetricRegistry
		consumerBatchSizeMetric metrics.Histogram
	)

	if metricRegistry != nil {
		consumerBatchSizeMetric = getOrRegisterHistogram("consumer-batch-size", metricRegistry)
	}

	// If request was throttled and empty we log and return without error
	if response.ThrottleTime != time.Duration(0) && len(response.Blocks) == 0 {
		Logger.Printf("consumer/broker/%d FetchResponse throttled %v\n", child.broker.broker.ID(), response.ThrottleTime)
		return nil, nil
	}

	// 获取 topic/partition 的数据
	block := response.GetBlock(child.topic, child.partition)

	// 如果没有数据，则返回 `ErrIncompleteResponse`
	if block == nil {
		return nil, ErrIncompleteResponse
	}

	// 报错
	if block.Err != ErrNoError {
		return nil, block.Err
	}

	// 数据条数
	nRecs, err := block.numRecords()
	if err != nil {
		return nil, err
	}

	// 统计上报
	consumerBatchSizeMetric.Update(int64(nRecs))

	// 如果 broker 返回值中包含从节点 broker id ，保存下来，当主 broker 节点不可用时，可临时使用从节点读取消息。
	child.preferredReadReplica = block.PreferredReadReplica

	// 如果数据条数为 0
	if nRecs == 0 {

		//
		partialTrailingMessage, err := block.isPartial()
		if err != nil {
			return nil, err
		}

		// We got no messages. If we got a trailing one then we need to ask for more data.
		// Otherwise we just poll again and wait for one to be produced...
		if partialTrailingMessage {
			if child.conf.Consumer.Fetch.Max > 0 && child.fetchSize == child.conf.Consumer.Fetch.Max {
				// we can't ask for more data, we've hit the configured limit
				child.sendError(ErrMessageTooLarge)
				child.offset++ // skip this one so we can keep processing future messages
			} else {
				child.fetchSize *= 2
				// check int32 overflow
				if child.fetchSize < 0 {
					child.fetchSize = math.MaxInt32
				}
				if child.conf.Consumer.Fetch.Max > 0 && child.fetchSize > child.conf.Consumer.Fetch.Max {
					child.fetchSize = child.conf.Consumer.Fetch.Max
				}
			}
		}

		return nil, nil
	}

	// we got messages, reset our fetch size in case it was increased for a previous request
	child.fetchSize = child.conf.Consumer.Fetch.Default							// 重置 fetch size
	atomic.StoreInt64(&child.highWaterMarkOffset, block.HighWaterMarkOffset) 	// 保存当前高水位信息

	// abortedProducerIDs contains producerID which message should be ignored as uncommitted
	// - producerID are added when the partitionConsumer iterate over the offset at which an aborted transaction begins (abortedTransaction.FirstOffset)
	// - producerID are removed when partitionConsumer iterate over an aborted controlRecord, meaning the aborted transaction for this producer is over
	//
	//
	abortedProducerIDs := make(map[int64]struct{}, len(block.AbortedTransactions))
	abortedTransactions := block.getAbortedTransactions()

	var messages []*ConsumerMessage
	for _, records := range block.RecordsSet {
		switch records.recordsType {
		// 在版本 >=2 时 producer 调用 DefaultRecord 进行写数据，否则使用 LegacyRecord 进行写数据
		case legacyRecords:
			// 解析 messages
			messageSetMessages, err := child.parseMessages(records.MsgSet)
			if err != nil {
				return nil, err
			}
			// 保存 messages
			messages = append(messages, messageSetMessages...)
		case defaultRecords:
			// Consume remaining abortedTransaction up to last offset of current batch
			for _, txn := range abortedTransactions {
				if txn.FirstOffset > records.RecordBatch.LastOffset() {
					break
				}
				abortedProducerIDs[txn.ProducerID] = struct{}{}
				// Pop abortedTransactions so that we never add it again
				abortedTransactions = abortedTransactions[1:]
			}

			// 解析 messages: 把 records.RecordBatch 转换成 []*ConsumerMessage
			recordBatchMessages, err := child.parseRecords(records.RecordBatch)
			if err != nil {
				return nil, err
			}

			// Parse and commit offset but do not expose messages that are:
			// - control records
			// - part of an aborted transaction when set to `ReadCommitted`
			//
			// 解析和提交偏移量，但不公开以下消息:
			//  控制记录
			//  当设置为 “ReadCommitted” 时，部分终止的事务


			// control record
			// 控制记录
			isControl, err := records.isControl()
			if err != nil {
				// I don't know why there is this continue in case of error to begin with
				// Safe bet is to ignore control messages if ReadUncommitted
				// and block on them in case of error and ReadCommitted
				if child.conf.Consumer.IsolationLevel == ReadCommitted {
					return nil, err
				}
				continue
			}

			if isControl {
				controlRecord, err := records.getControlRecord()
				if err != nil {
					return nil, err
				}
				if controlRecord.Type == ControlRecordAbort {
					delete(abortedProducerIDs, records.RecordBatch.ProducerID)
				}
				continue
			}

			// filter aborted transactions
			if child.conf.Consumer.IsolationLevel == ReadCommitted {
				_, isAborted := abortedProducerIDs[records.RecordBatch.ProducerID]
				if records.RecordBatch.IsTransactional && isAborted {
					continue
				}
			}

			messages = append(messages, recordBatchMessages...)
		default:
			return nil, fmt.Errorf("unknown records type: %v", records.recordsType)
		}
	}

	return messages, nil
}






// brokerConsumer 是做什么用的呢？
//
// 当多个 Consumer 需要从一个 Broker 订阅多个 Topic 时，不会创建多个连接，
// 仅使用一个 Broker 连接来消费数据，再将数据按 partition 分给不同的 partitionConsumer。
//
type brokerConsumer struct {
	consumer         *consumer
	broker           *Broker
	input            chan *partitionConsumer		// 无缓冲队列
	newSubscriptions chan []*partitionConsumer		// 无缓冲队列
	subscriptions    map[*partitionConsumer]none
	wait             chan none
	acks             sync.WaitGroup	//
	refs             int
}

func (c *consumer) newBrokerConsumer(broker *Broker) *brokerConsumer {

	bc := &brokerConsumer{
		consumer:         c,
		broker:           broker,
		input:            make(chan *partitionConsumer),
		newSubscriptions: make(chan []*partitionConsumer),
		wait:             make(chan none),
		subscriptions:    make(map[*partitionConsumer]none),
		refs:             0,
	}

	// 从 brokerConsumer.input 中接收新订阅的 partitionConsumer 并将其传递给 subscriptionConsumer() 协程，
	// 并源源不断的触发 subscriptionConsumer() 协程执行消息拉取和分发逻辑。
	go withRecover(bc.subscriptionManager)
	// 从 brokerConsumer.newSubscriptions 中接收新加入订阅的 partitionConsumer ，执行消息拉取和分发逻辑。
	go withRecover(bc.subscriptionConsumer)

	return bc
}

// The subscriptionManager constantly accepts new subscriptions on `input` (even when the main subscriptionConsumer
// goroutine is in the middle of a network request) and batches it up. The main worker goroutine picks
// up a batch of new subscriptions between every network request by reading from `newSubscriptions`, so we give
// it nil if no new subscriptions are available. We also write to `wait` only when new subscriptions is available,
// so the main goroutine can block waiting for work if it has none.
//
//
// subscriptionManager() 协程负责从 brokerConsumer.input 中接收新订阅的 partitionConsumer 并将其传递给 subscriptionConsumer() 。
//
// subscriptionManager() 协程逻辑比较绕，是因为它不仅负责接收新订阅的 partitionConsumers 并传递给 subscriptionConsumer() 协程，
// 还需要持续不断的触发 subscriptionConsumer() 协程去 broker 拉取消息和回调通知给各个 partitionConsumers ，而且这两个逻辑合二为一，
// 显得比较复杂。
//
func (bc *brokerConsumer) subscriptionManager() {

	// 缓存一批新加入订阅的 partitionConsumers 发送给 subscriptionConsumer() 协程
	var pcs []*partitionConsumer

	for {
		if len(pcs) > 0 {
			// 如果有新订阅的 partitionSconsumers ，就将其写入到 bc.newSubscriptions 管道中。
			// 同步写入过程:
			// 	1. 从 input 管道中读取新加入订阅的 partitionConsumer 并缓存到本地
			// 	2. 将已缓存的 partitionConsumers 写入 bc.newSubscriptions 管道，执行消息拉取和回调
			// 	3. 将 none{} 对象写入到 bc.wait 管道
			// 由于 bc.newSubscriptions 管道是无缓冲的，写入可能会发生阻塞，此时依赖 bc.wait 来同步。
			select {
			// 监听 bc.input 获取新加入订阅的 partitionConsumer 并保存到批量缓存中
			case pc, ok := <-bc.input:
				if !ok {
					goto done
				}
				pcs = append(pcs, pc)
			// 若没有新加入的	partitionConsumer 就把已缓存的数据发送给 subscriptionConsumer() 协程
			case bc.newSubscriptions <- pcs:
				pcs = nil
			// [如果] 如果 bc.wait 被触发，意味着有新加入订阅的 partitionConsumers 等待写入 bc.newSubscriptions 管道，但是发生了阻塞。
			case bc.wait <- none{}:
			}
		} else {
			// [重要] 如果无新订阅的 partitionSconsumers ，就写入 nil 到 bc.newSubscriptions 管道中。
			// 为何要持续不断的写入 nil 呢？目的就是触发 subscriptionConsumer() 协程不断的执行消息拉取和回调通知，
			// 使消息不断的从 broker 拉取并发送给各个订阅的 partitionConsumers 。
			select {
			// 监听 bc.input 获取新加入订阅的 partitionConsumer 并保存到批量缓存中；
			case pc, ok := <-bc.input:
				if !ok {
					goto done
				}
				pcs = append(pcs, pc)
			// 若没有新加入的	partitionConsumers 就把 nil 发送到 bc.newSubscriptions 管道，通知给 subscriptionConsumer() 协程，
			// 因为 bc.newSubscriptions 管道是无缓冲的，所以当 subscriptionConsumer() 协程在执行消息拉取和分发过程中，本处写管道操作
			// 会被阻塞，直到 subscriptionConsumer() 协程完成当前轮次的拉取和分发。
			case bc.newSubscriptions <- nil:
			}

		}
	}

done:
	close(bc.wait)
	if len(pcs) > 0 {
		bc.newSubscriptions <- pcs
	}
	close(bc.newSubscriptions)
}

//subscriptionConsumer ensures we will get nil right away if no new subscriptions is available
//
//
// 把新关联的 partitionConsumer 保存起来。获取所有partitionConsumer的消息，并把消息传递给partitionConsumer的feeder，由此驱动partitionConsumer的responseFeeder被驱动执行。当该brokerConsumer的所有partitionConsumer都处理完了这个响应消息之后，brokerConsumer就检查所有partitionConsumer的处理结果，并决定是否触发partitionConsumer的dispatcher。
//
//
// 循环发起 FetchRequest，向 broker 拉取一批消息，协调发给 partitionConsumer；
func (bc *brokerConsumer) subscriptionConsumer() {

	// 优先读取 bc.wait 管道，这意味着已经有新订阅 partitionConsumers 等待执行
	<-bc.wait // wait for our first piece of work

	// 从管道中不断获取新订阅的 partitionConsumers ，在 for 执行过程中，bc.newSubscriptions 会阻塞写操作，
	for newSubscriptions := range bc.newSubscriptions {

		// 更新到本地缓存中
		bc.updateSubscriptions(newSubscriptions)

		// 如果 bc.subscriptions 为空 ，意味着不存在正在订阅中的 partitionConsumers ，
		// 则本 brokerConsumer 即将被关闭或者正在等待新订阅者，不需拉取消息，进入等待中。
		if len(bc.subscriptions) == 0 {
			// We're about to be shut down or we're about to receive more subscriptions.
			// Either way, the signal just hasn't propagated to our goroutine yet.
			//
			// 如果 bc.wait 被触发，意味着在有新加入订阅的 partitionConsumers 正在等待被处理。
			<-bc.wait
			continue
		}

		// [核心] 至此，有 partitionConsumers 正在订阅中，需要去 broker 拉取消息并回调通知给各个 partitionConsumers 。
		response, err := bc.fetchNewMessages()
		if err != nil {
			Logger.Printf("consumer/broker/%d disconnecting due to error processing FetchRequest: %s\n", bc.broker.ID(), err)
			bc.abort(err)
			return
		}

		// 把 response 通过 child.feeder 管道发送给每个 subscriptions
		bc.acks.Add(len(bc.subscriptions))
		for child := range bc.subscriptions {
			child.feeder <- response
		}

		// 同步等待每个 subscriptions 处理完成
		bc.acks.Wait()

		// subscription 会把处理结果保存在 child.responseResult 上，检查是否有错误并进行处理
		bc.handleResponses()
	}
}


//
//
//
//
//
//
func (bc *brokerConsumer) updateSubscriptions(newSubscriptions []*partitionConsumer) {

	// 把新订阅的 partitionConsumers 更新到本地 map 中
	for _, child := range newSubscriptions {
		bc.subscriptions[child] = none{}
		Logger.Printf("consumer/broker/%d added subscription to %s/%d\n", bc.broker.ID(), child.topic, child.partition)
	}

	// 遍历本地缓存的 partitionConsumers ，若发现有已经关闭的，执行清理
	for child := range bc.subscriptions {
		select {
		case <-child.dying:
			Logger.Printf("consumer/broker/%d closed dead subscription to %s/%d\n", bc.broker.ID(), child.topic, child.partition)
			close(child.trigger)			// 关闭 child 的消息消费协程
			delete(bc.subscriptions, child) // 解除 child 的消息订阅
		default:
			// no-op
		}
	}
}

// handleResponses handles the response codes left for us by our subscriptions, and abandons ones that have been closed
//
//
// [重要]
//
func (bc *brokerConsumer) handleResponses() {

	// 遍历每个 subscription
	for child := range bc.subscriptions {

		// 获取 response 的处理结果，并重置
		result := child.responseResult; child.responseResult = nil

		// 未出错直接 continue
		if result == nil {
			// [暂时忽略]
			if child.preferredReadReplica >= 0 && bc.broker.ID() != child.preferredReadReplica {
				// not an error but needs redispatching to consume from prefered replica
				child.trigger <- none{}
				delete(bc.subscriptions, child)
			}
			continue
		}

		// Discard any replica preference.
		child.preferredReadReplica = -1

		switch result {
		// 消费超时，意味着用户迟迟没有从 child.messages 管道中读取消息，解除订阅关系
		case errTimedOut:
			Logger.Printf("consumer/broker/%d abandoned subscription to %s/%d because consuming was taking too long\n", bc.broker.ID(), child.topic, child.partition)
			// 解除订阅关系，不会再拉取本 partitionConsumer 的消息和回调
			delete(bc.subscriptions, child)

		// [严重错误] offset 超过 broker 保存的范围，重试也无济于事，直接停止 partitionConsumer ，并通知错误给用户。
		case ErrOffsetOutOfRange:
			// there's no point in retrying this it will just fail the same way again
			// shut it down and force the user to choose what to do
			// 重试没有意义，它只会以同样的方式失败，关闭它并迫使用户做选择

			// 把错误写入 errors 管道通知用户
			child.sendError(result)
			Logger.Printf("consumer/%s/%d shutting down because %s\n", child.topic, child.partition, result)
			// [重要] close(child.trigger) 会触发 child.dispatcher() 和 child.responseFeeder() 协程退出，结束订阅。
			close(child.trigger)
			// 解除订阅信息，不会再拉取本 partitionConsumer 的消息和回调
			delete(bc.subscriptions, child)

		// 服务端错误
		case ErrUnknownTopicOrPartition, ErrNotLeaderForPartition, ErrLeaderNotAvailable, ErrReplicaNotAvailable:
			// not an error, but does need redispatching
			Logger.Printf("consumer/broker/%d abandoned subscription to %s/%d because %s\n", bc.broker.ID(), child.topic, child.partition, result)
			// 写 child.trigger 管道触发 child 重新选择 broker 并重新加入订阅
			child.trigger <- none{}
			// 解除订阅信息，不会再拉取本 partitionConsumer 的消息和回调
			delete(bc.subscriptions, child)

		// 其它错误
		default:
			// dunno, tell the user and try redispatching
			// 将错误信息通知用户
			child.sendError(result)
			Logger.Printf("consumer/broker/%d abandoned subscription to %s/%d because %s\n", bc.broker.ID(), child.topic, child.partition, result)
			// 写 child.trigger 管道触发 child 重新选择 broker 并重新加入订阅
			child.trigger <- none{}
			// 解除订阅信息，不会再拉取本 partitionConsumer 的消息和回调
			delete(bc.subscriptions, child)
		}
	}
}

func (bc *brokerConsumer) abort(err error) {

	bc.consumer.abandonBrokerConsumer(bc)
	_ = bc.broker.Close() // we don't care about the error this might return, we already have one

	for child := range bc.subscriptions {
		child.sendError(err)
		child.trigger <- none{}
	}

	for newSubscriptions := range bc.newSubscriptions {
		if len(newSubscriptions) == 0 {
			<-bc.wait
			continue
		}
		for _, child := range newSubscriptions {
			child.sendError(err)
			child.trigger <- none{}
		}
	}
}

// 从 broker 拉取一批消息
func (bc *brokerConsumer) fetchNewMessages() (*FetchResponse, error) {

	// 构造消息拉取请求
	request := &FetchRequest{
		MinBytes:    bc.consumer.conf.Consumer.Fetch.Min,
		MaxWaitTime: int32(bc.consumer.conf.Consumer.MaxWaitTime / time.Millisecond),
	}

	// 参数拼装
	if bc.consumer.conf.Version.IsAtLeast(V0_9_0_0) {
		request.Version = 1
	}

	if bc.consumer.conf.Version.IsAtLeast(V0_10_0_0) {
		request.Version = 2
	}

	if bc.consumer.conf.Version.IsAtLeast(V0_10_1_0) {
		request.Version = 3
		request.MaxBytes = MaxResponseSize
	}

	if bc.consumer.conf.Version.IsAtLeast(V0_11_0_0) {
		request.Version = 4
		request.Isolation = bc.consumer.conf.Consumer.IsolationLevel
	}

	if bc.consumer.conf.Version.IsAtLeast(V1_1_0_0) {
		request.Version = 7
		// We do not currently implement KIP-227 FetchSessions. Setting the id to 0
		// and the epoch to -1 tells the broker not to generate as session ID we're going
		// to just ignore anyway.
		request.SessionID = 0
		request.SessionEpoch = -1
	}

	if bc.consumer.conf.Version.IsAtLeast(V2_1_0_0) {
		request.Version = 10
	}

	if bc.consumer.conf.Version.IsAtLeast(V2_3_0_0) {
		request.Version = 11
		request.RackID = bc.consumer.conf.RackID
	}

	// 添加消息拉取的 topic/partition/offset/size 详情
	for child := range bc.subscriptions {
		request.AddBlock(child.topic, child.partition, child.offset, child.fetchSize)
	}

	// 执行消息拉取
	return bc.broker.Fetch(request)
}
