package sarama

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Client is a generic Kafka client. It manages connections to one or more Kafka brokers.
// You MUST call Close() on a client to avoid leaks, it will not be garbage-collected
// automatically when it passes out of scope. It is safe to share a client amongst many
// users, however Kafka will process requests from a single client strictly in serial,
// so it is generally more efficient to use the default one client per producer/consumer.
//
// Client 是一个通用的 Kafka 客户端，它管理到一个或多个 Kafka broker 的连接。
// 必须在 Client 上调用 Close() 以避免泄漏，因为当它超出作用域时，不会自动进行垃圾收集。
// Client 是协程安全的，多用户可以共享一个 Client ，但是 Kafka 会严格地以串行的方式处理来自单个 Client 的请求，
// 所以使用独立的 Client 往往是更高效的。
type Client interface {

	// Config returns the Config struct of the client. This struct should not be
	// altered after it has been created.
	Config() *Config

	// Controller returns the cluster controller broker. It will return a
	// locally cached value if it's available. You can call RefreshController
	// to update the cached value. Requires Kafka 0.10 or higher.
	Controller() (*Broker, error)

	// RefreshController retrieves the cluster controller from fresh metadata
	// and stores it in the local cache. Requires Kafka 0.10 or higher.
	RefreshController() (*Broker, error)

	// Brokers returns the current set of active brokers as retrieved from cluster metadata.
	Brokers() []*Broker

	// Broker returns the active Broker if available for the broker ID.
	Broker(brokerID int32) (*Broker, error)

	// Topics returns the set of available topics as retrieved from cluster metadata.
	Topics() ([]string, error)

	// Partitions returns the sorted list of all partition IDs for the given topic.
	Partitions(topic string) ([]int32, error)

	// WritablePartitions returns the sorted list of all writable partition IDs for
	// the given topic, where "writable" means "having a valid leader accepting
	// writes".
	WritablePartitions(topic string) ([]int32, error)

	// Leader returns the broker object that is the leader of the current
	// topic/partition, as determined by querying the cluster metadata.
	Leader(topic string, partitionID int32) (*Broker, error)

	// Replicas returns the set of all replica IDs for the given partition.
	Replicas(topic string, partitionID int32) ([]int32, error)

	// InSyncReplicas returns the set of all in-sync replica IDs for the given
	// partition. In-sync replicas are replicas which are fully caught up with
	// the partition leader.
	InSyncReplicas(topic string, partitionID int32) ([]int32, error)

	// OfflineReplicas returns the set of all offline replica IDs for the given
	// partition. Offline replicas are replicas which are offline
	OfflineReplicas(topic string, partitionID int32) ([]int32, error)

	// RefreshBrokers takes a list of addresses to be used as seed brokers.
	// Existing broker connections are closed and the updated list of seed brokers
	// will be used for the next metadata fetch.
	RefreshBrokers(addrs []string) error

	// RefreshMetadata takes a list of topics and queries the cluster to refresh the
	// available metadata for those topics. If no topics are provided, it will refresh
	// metadata for all topics.
	RefreshMetadata(topics ...string) error

	// GetOffset queries the cluster to get the most recent available offset at the
	// given time (in milliseconds) on the topic/partition combination.
	// Time should be OffsetOldest for the earliest available offset,
	// OffsetNewest for the offset of the message that will be produced next, or a time.
	GetOffset(topic string, partitionID int32, time int64) (int64, error)

	// Coordinator returns the coordinating broker for a consumer group. It will
	// return a locally cached value if it's available. You can call
	// RefreshCoordinator to update the cached value. This function only works on
	// Kafka 0.8.2 and higher.
	Coordinator(consumerGroup string) (*Broker, error)

	// RefreshCoordinator retrieves the coordinator for a consumer group and stores it
	// in local cache. This function only works on Kafka 0.8.2 and higher.
	RefreshCoordinator(consumerGroup string) error

	// InitProducerID retrieves information required for Idempotent Producer
	InitProducerID() (*InitProducerIDResponse, error)

	// Close shuts down all broker connections managed by this client. It is required
	// to call this function before a client object passes out of scope, as it will
	// otherwise leak memory. You must close any Producers or Consumers using a client
	// before you close the client.
	Close() error

	// Closed returns true if the client has already had Close called on it
	Closed() bool
}

const (
	// OffsetNewest stands for the log head offset, i.e. the offset that will be
	// assigned to the next message that will be produced to the partition. You
	// can send this to a client's GetOffset method to get this offset, or when
	// calling ConsumePartition to start consuming new messages.
	OffsetNewest int64 = -1
	// OffsetOldest stands for the oldest offset available on the broker for a
	// partition. You can send this to a client's GetOffset method to get this
	// offset, or when calling ConsumePartition to start consuming from the
	// oldest offset that is still available on the broker.
	OffsetOldest int64 = -2
)

type client struct {
	conf           *Config
	closer, closed chan none // for shutting down background metadata updater

	// the broker addresses given to us through the constructor are not guaranteed to be returned in
	// the cluster metadata (I *think* it only returns brokers who are currently leading partitions?)
	// so we store them separately
	seedBrokers []*Broker
	deadSeeds   []*Broker

	controllerID   int32                                   // cluster controller broker id
	brokers        map[int32]*Broker                       // maps broker ids to brokers
	metadata       map[string]map[int32]*PartitionMetadata // maps topics to partition ids to metadata
	metadataTopics map[string]none                         // topics that need to collect metadata
	coordinators   map[string]int32                        // Maps consumer group names to coordinating broker IDs

	// If the number of partitions is large, we can get some churn calling cachedPartitions,
	// so the result is cached.  It is important to update this value whenever metadata is changed
	cachedPartitionsResults map[string][maxPartitionIndex][]int32

	lock sync.RWMutex // protects access to the maps that hold cluster state.
}

// NewClient creates a new Client. It connects to one of the given broker addresses
// and uses that broker to automatically fetch metadata on the rest of the kafka cluster.
// If metadata cannot be retrieved from any of the given broker addresses, the client is not created.
//
//
//
// client 用于
//
func NewClient(addrs []string, conf *Config) (Client, error) {
	Logger.Println("Initializing new client")

	if conf == nil {
		conf = NewConfig()
	}

	if err := conf.Validate(); err != nil {
		return nil, err
	}

	if len(addrs) < 1 {
		return nil, ConfigurationError("You must provide at least one broker address")
	}

	// 构造客户端对象
	client := &client{
		conf:                    conf,
		closer:                  make(chan none),
		closed:                  make(chan none),
		brokers:                 make(map[int32]*Broker),
		metadata:                make(map[string]map[int32]*PartitionMetadata),
		metadataTopics:          make(map[string]none),
		cachedPartitionsResults: make(map[string][maxPartitionIndex][]int32),
		coordinators:            make(map[string]int32),
	}

	// 打乱 addrs 的顺序
	client.randomizeSeedBrokers(addrs)

	// 是否需要缓存所有 Topic 的元数据
	if conf.Metadata.Full {
		// do an initial fetch of all cluster metadata by specifying an empty list of topics
		// 从 broker 拉取 metadata ，过程中会首先通过 open 创建到 broker 的连接。
		err := client.RefreshMetadata()
		switch err {
		case nil:
			break
		case ErrLeaderNotAvailable, ErrReplicaNotAvailable, ErrTopicAuthorizationFailed, ErrClusterAuthorizationFailed:
			// indicates that maybe part of the cluster is down, but is not fatal to creating the client
			Logger.Println(err)
		default:
			close(client.closed) // we haven't started the background updater yet, so we have to do this manually
			_ = client.Close()
			return nil, err
		}
	}

	// 启动定时拉取和刷新 metadata 的协程
	go withRecover(client.backgroundMetadataUpdater)

	Logger.Println("Successfully initialized new client")
	return client, nil
}

func (client *client) Config() *Config {
	return client.conf
}

func (client *client) Brokers() []*Broker {
	client.lock.RLock()
	defer client.lock.RUnlock()
	brokers := make([]*Broker, 0, len(client.brokers))
	for _, broker := range client.brokers {
		brokers = append(brokers, broker)
	}
	return brokers
}

func (client *client) Broker(brokerID int32) (*Broker, error) {
	client.lock.RLock()
	defer client.lock.RUnlock()
	broker, ok := client.brokers[brokerID]
	if !ok {
		return nil, ErrBrokerNotFound
	}
	_ = broker.Open(client.conf)
	return broker, nil
}

func (client *client) InitProducerID() (*InitProducerIDResponse, error) {
	var err error
	for broker := client.any(); broker != nil; broker = client.any() {
		req := &InitProducerIDRequest{}

		response, err := broker.InitProducerID(req)
		switch err.(type) {
		case nil:
			return response, nil
		default:
			// some error, remove that broker and try again
			Logger.Printf("Client got error from broker %d when issuing InitProducerID : %v\n", broker.ID(), err)
			_ = broker.Close()
			client.deregisterBroker(broker)
		}
	}
	return nil, err
}

func (client *client) Close() error {
	if client.Closed() {
		// Chances are this is being called from a defer() and the error will go unobserved
		// so we go ahead and log the event in this case.
		Logger.Printf("Close() called on already closed client")
		return ErrClosedClient
	}

	// shutdown and wait for the background thread before we take the lock, to avoid races
	close(client.closer)
	<-client.closed

	client.lock.Lock()
	defer client.lock.Unlock()
	Logger.Println("Closing Client")

	for _, broker := range client.brokers {
		safeAsyncClose(broker)
	}

	for _, broker := range client.seedBrokers {
		safeAsyncClose(broker)
	}

	client.brokers = nil
	client.metadata = nil
	client.metadataTopics = nil

	return nil
}

func (client *client) Closed() bool {
	client.lock.RLock()
	defer client.lock.RUnlock()

	return client.brokers == nil
}

func (client *client) Topics() ([]string, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	client.lock.RLock()
	defer client.lock.RUnlock()

	ret := make([]string, 0, len(client.metadata))
	for topic := range client.metadata {
		ret = append(ret, topic)
	}

	return ret, nil
}

func (client *client) MetadataTopics() ([]string, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	client.lock.RLock()
	defer client.lock.RUnlock()

	ret := make([]string, 0, len(client.metadataTopics))
	for topic := range client.metadataTopics {
		ret = append(ret, topic)
	}

	return ret, nil
}

func (client *client) Partitions(topic string) ([]int32, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	// 从本地缓存拉取
	partitions := client.cachedPartitions(topic, allPartitions)

	// 本地缓存不存在，则先刷新本地缓存，再从缓存中获取
	if len(partitions) == 0 {
		err := client.RefreshMetadata(topic)
		if err != nil {
			return nil, err
		}
		partitions = client.cachedPartitions(topic, allPartitions)
	}

	// no partitions found after refresh metadata
	// 没有数据，报错
	if len(partitions) == 0 {
		return nil, ErrUnknownTopicOrPartition
	}

	// 返回数据
	return partitions, nil
}

func (client *client) WritablePartitions(topic string) ([]int32, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	partitions := client.cachedPartitions(topic, writablePartitions)

	// len==0 catches when it's nil (no such topic) and the odd case when every single
	// partition is undergoing leader election simultaneously. Callers have to be able to handle
	// this function returning an empty slice (which is a valid return value) but catching it
	// here the first time (note we *don't* catch it below where we return ErrUnknownTopicOrPartition) triggers
	// a metadata refresh as a nicety so callers can just try again and don't have to manually
	// trigger a refresh (otherwise they'd just keep getting a stale cached copy).
	if len(partitions) == 0 {
		err := client.RefreshMetadata(topic)
		if err != nil {
			return nil, err
		}
		partitions = client.cachedPartitions(topic, writablePartitions)
	}

	if partitions == nil {
		return nil, ErrUnknownTopicOrPartition
	}

	return partitions, nil
}

func (client *client) Replicas(topic string, partitionID int32) ([]int32, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	metadata := client.cachedMetadata(topic, partitionID)

	if metadata == nil {
		err := client.RefreshMetadata(topic)
		if err != nil {
			return nil, err
		}
		metadata = client.cachedMetadata(topic, partitionID)
	}

	if metadata == nil {
		return nil, ErrUnknownTopicOrPartition
	}

	if metadata.Err == ErrReplicaNotAvailable {
		return dupInt32Slice(metadata.Replicas), metadata.Err
	}
	return dupInt32Slice(metadata.Replicas), nil
}

func (client *client) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	metadata := client.cachedMetadata(topic, partitionID)

	if metadata == nil {
		err := client.RefreshMetadata(topic)
		if err != nil {
			return nil, err
		}
		metadata = client.cachedMetadata(topic, partitionID)
	}

	if metadata == nil {
		return nil, ErrUnknownTopicOrPartition
	}

	if metadata.Err == ErrReplicaNotAvailable {
		return dupInt32Slice(metadata.Isr), metadata.Err
	}
	return dupInt32Slice(metadata.Isr), nil
}

func (client *client) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	metadata := client.cachedMetadata(topic, partitionID)

	if metadata == nil {
		err := client.RefreshMetadata(topic)
		if err != nil {
			return nil, err
		}
		metadata = client.cachedMetadata(topic, partitionID)
	}

	if metadata == nil {
		return nil, ErrUnknownTopicOrPartition
	}

	if metadata.Err == ErrReplicaNotAvailable {
		return dupInt32Slice(metadata.OfflineReplicas), metadata.Err
	}
	return dupInt32Slice(metadata.OfflineReplicas), nil
}

func (client *client) Leader(topic string, partitionID int32) (*Broker, error) {

	if client.Closed() {
		return nil, ErrClosedClient
	}

	// 从缓存中获取 topic/partition 的 leader broker 对象
	leader, err := client.cachedLeader(topic, partitionID)

	// 如果缓存为空，则回源刷新缓存，再重试从缓存中获取
	if leader == nil {
		err = client.RefreshMetadata(topic)
		if err != nil {
			return nil, err
		}
		leader, err = client.cachedLeader(topic, partitionID)
	}

	return leader, err
}

func (client *client) RefreshBrokers(addrs []string) error {
	if client.Closed() {
		return ErrClosedClient
	}

	client.lock.Lock()
	defer client.lock.Unlock()

	for _, broker := range client.brokers {
		_ = broker.Close()
		delete(client.brokers, broker.ID())
	}

	client.seedBrokers = nil
	client.deadSeeds = nil

	client.randomizeSeedBrokers(addrs)

	return nil
}


// 刷新本地缓存的 Metadata 元数据
func (client *client) RefreshMetadata(topics ...string) error {

	// 已关闭
	if client.Closed() {
		return ErrClosedClient
	}

	// Prior to 0.8.2, Kafka will throw exceptions on an empty topic and not return a proper
	// error. This handles the case by returning an error instead of sending it
	// off to Kafka. See: https://github.com/Shopify/sarama/pull/38#issuecomment-26362310
	//
	// 参数检查，不能接受空 topic
	for _, topic := range topics {
		if len(topic) == 0 {
			return ErrInvalidTopic // this is the error that 0.8.2 and later correctly return
		}
	}

	// 超时控制
	deadline := time.Time{}
	if client.conf.Metadata.Timeout > 0 {
		deadline = time.Now().Add(client.conf.Metadata.Timeout)
	}

	// 刷新本地缓存的 Metadata 元数据
	return client.tryRefreshMetadata(topics, client.conf.Metadata.Retry.Max, deadline)
}

func (client *client) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	if client.Closed() {
		return -1, ErrClosedClient
	}

	// 请求 broker 查询 offset ，若查询出错，刷新元数据缓存后再重试一次
	offset, err := client.getOffset(topic, partitionID, time)
	if err != nil {
		if err := client.RefreshMetadata(topic); err != nil {
			return -1, err
		}
		return client.getOffset(topic, partitionID, time)
	}

	return offset, err
}

func (client *client) Controller() (*Broker, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	if !client.conf.Version.IsAtLeast(V0_10_0_0) {
		return nil, ErrUnsupportedVersion
	}

	// 根据 client.controllerID 获取缓存的 broker 对象，若获取失败，刷新元数据缓存后再重试一次
	controller := client.cachedController()
	if controller == nil {
		if err := client.refreshMetadata(); err != nil {
			return nil, err
		}
		controller = client.cachedController()
	}

	if controller == nil {
		return nil, ErrControllerNotAvailable
	}

	// 获取 controller 成功，则打开连接并返回
	_ = controller.Open(client.conf)
	return controller, nil
}

// deregisterController removes the cached controllerID
func (client *client) deregisterController() {
	client.lock.Lock()
	defer client.lock.Unlock()
	delete(client.brokers, client.controllerID)
}

// RefreshController retrieves the cluster controller from fresh metadata
// and stores it in the local cache. Requires Kafka 0.10 or higher.
func (client *client) RefreshController() (*Broker, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	client.deregisterController()

	if err := client.refreshMetadata(); err != nil {
		return nil, err
	}

	controller := client.cachedController()
	if controller == nil {
		return nil, ErrControllerNotAvailable
	}

	_ = controller.Open(client.conf)
	return controller, nil
}

func (client *client) Coordinator(consumerGroup string) (*Broker, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	coordinator := client.cachedCoordinator(consumerGroup)

	if coordinator == nil {
		if err := client.RefreshCoordinator(consumerGroup); err != nil {
			return nil, err
		}
		coordinator = client.cachedCoordinator(consumerGroup)
	}

	if coordinator == nil {
		return nil, ErrConsumerCoordinatorNotAvailable
	}

	_ = coordinator.Open(client.conf)
	return coordinator, nil
}

func (client *client) RefreshCoordinator(consumerGroup string) error {
	if client.Closed() {
		return ErrClosedClient
	}

	response, err := client.getConsumerMetadata(consumerGroup, client.conf.Metadata.Retry.Max)
	if err != nil {
		return err
	}

	client.lock.Lock()
	defer client.lock.Unlock()
	client.registerBroker(response.Coordinator)
	client.coordinators[consumerGroup] = response.Coordinator.ID()
	return nil
}

// private broker management helpers
//
// 打乱用户输入的 addrs 的顺序，保存到 seedBrokers 中
func (client *client) randomizeSeedBrokers(addrs []string) {
	// 把用户输入的 broker 地址作为 “种子broker” 增加到 seedBrokers 中
	// 随后客户端会根据已有的 broker 地址，自动刷新元数据，以获取更多的 broker 地址，所以称之为种子
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, index := range random.Perm(len(addrs)) {
		client.seedBrokers = append(client.seedBrokers, NewBroker(addrs[index]))
	}
}

func (client *client) updateBroker(brokers []*Broker) {
	var currentBroker = make(map[int32]*Broker, len(brokers))

	for _, broker := range brokers {
		currentBroker[broker.ID()] = broker
		if client.brokers[broker.ID()] == nil { // add new broker
			client.brokers[broker.ID()] = broker
			Logger.Printf("client/brokers registered new broker #%d at %s", broker.ID(), broker.Addr())
		} else if broker.Addr() != client.brokers[broker.ID()].Addr() { // replace broker with new address
			safeAsyncClose(client.brokers[broker.ID()])
			client.brokers[broker.ID()] = broker
			Logger.Printf("client/brokers replaced registered broker #%d with %s", broker.ID(), broker.Addr())
		}
	}

	for id, broker := range client.brokers {
		if _, exist := currentBroker[id]; !exist { // remove old broker
			safeAsyncClose(broker)
			delete(client.brokers, id)
			Logger.Printf("client/broker remove invalid broker #%d with %s", broker.ID(), broker.Addr())
		}
	}
}

// registerBroker makes sure a broker received by a Metadata or Coordinator request is registered
// in the brokers map. It returns the broker that is registered, which may be the provided broker,
// or a previously registered Broker instance. You must hold the write lock before calling this function.
func (client *client) registerBroker(broker *Broker) {
	if client.brokers == nil {
		Logger.Printf("cannot register broker #%d at %s, client already closed", broker.ID(), broker.Addr())
		return
	}

	if client.brokers[broker.ID()] == nil {
		client.brokers[broker.ID()] = broker
		Logger.Printf("client/brokers registered new broker #%d at %s", broker.ID(), broker.Addr())
	} else if broker.Addr() != client.brokers[broker.ID()].Addr() {
		safeAsyncClose(client.brokers[broker.ID()])
		client.brokers[broker.ID()] = broker
		Logger.Printf("client/brokers replaced registered broker #%d with %s", broker.ID(), broker.Addr())
	}
}

// deregisterBroker removes a broker from the seedsBroker list, and if it's
// not the seedbroker, removes it from brokers map completely.
func (client *client) deregisterBroker(broker *Broker) {

	client.lock.Lock()
	defer client.lock.Unlock()

	if len(client.seedBrokers) > 0 && broker == client.seedBrokers[0] {
		client.deadSeeds = append(client.deadSeeds, broker)
		client.seedBrokers = client.seedBrokers[1:]
	} else {
		// we do this so that our loop in `tryRefreshMetadata` doesn't go on forever,
		// but we really shouldn't have to; once that loop is made better this case can be
		// removed, and the function generally can be renamed from `deregisterBroker` to
		// `nextSeedBroker` or something
		Logger.Printf("client/brokers deregistered broker #%d at %s", broker.ID(), broker.Addr())
		delete(client.brokers, broker.ID())
	}
}

func (client *client) resurrectDeadBrokers() {
	client.lock.Lock()
	defer client.lock.Unlock()

	Logger.Printf("client/brokers resurrecting %d dead seed brokers", len(client.deadSeeds))
	client.seedBrokers = append(client.seedBrokers, client.deadSeeds...)
	client.deadSeeds = nil
}

// 选择一个可用的 broker
func (client *client) any() *Broker {
	client.lock.RLock()
	defer client.lock.RUnlock()

	// 如果设置了 seedBrokers ，那么就取出并打开 seedBroker （这些 seedBrokers 是用户传入的）
	if len(client.seedBrokers) > 0 {
		_ = client.seedBrokers[0].Open(client.conf)
		return client.seedBrokers[0]
	}

	// 如果未设置 seedBrokers ，就随机性选择其它的 broker ，这些 broker 可能是通过刷新元数据获取到的。
	// not guaranteed to be random *or* deterministic
	for _, broker := range client.brokers { // range map 不能保证是随机的或确定性的
		_ = broker.Open(client.conf)
		return broker
	}

	return nil
}

// private caching/lazy metadata helpers

type partitionType int

const (
	allPartitions partitionType = iota
	writablePartitions
	// If you add any more types, update the partition cache in update()

	// Ensure this is the last partition type value
	maxPartitionIndex
)

func (client *client) cachedMetadata(topic string, partitionID int32) *PartitionMetadata {
	client.lock.RLock()
	defer client.lock.RUnlock()

	partitions := client.metadata[topic]
	if partitions != nil {
		return partitions[partitionID]
	}

	return nil
}

func (client *client) cachedPartitions(topic string, partitionSet partitionType) []int32 {
	client.lock.RLock()
	defer client.lock.RUnlock()

	partitions, exists := client.cachedPartitionsResults[topic]

	if !exists {
		return nil
	}
	return partitions[partitionSet]
}

func (client *client) setPartitionCache(topic string, partitionSet partitionType) []int32 {
	partitions := client.metadata[topic]

	if partitions == nil {
		return nil
	}

	ret := make([]int32, 0, len(partitions))
	for _, partition := range partitions {
		if partitionSet == writablePartitions && partition.Err == ErrLeaderNotAvailable {
			continue
		}
		ret = append(ret, partition.ID)
	}

	sort.Sort(int32Slice(ret))
	return ret
}

func (client *client) cachedLeader(topic string, partitionID int32) (*Broker, error) {

	client.lock.RLock()
	defer client.lock.RUnlock()

	// 取出 topic 的元数据
	partitions := client.metadata[topic]
	if partitions != nil {
		// 取出 partition 的元数据
		metadata, ok := partitions[partitionID]
		if ok {
			// 状态检查
			if metadata.Err == ErrLeaderNotAvailable {
				return nil, ErrLeaderNotAvailable
			}
			// 根据 broker id 获取缓存的 broker 对象
			b := client.brokers[metadata.Leader]
			if b == nil {
				return nil, ErrLeaderNotAvailable
			}
			// 获取&创建同 broker 的连接，并返回
			_ = b.Open(client.conf)
			return b, nil
		}
	}

	return nil, ErrUnknownTopicOrPartition
}

func (client *client) getOffset(topic string, partitionID int32, time int64) (int64, error) {

	// 获取 topic/partition 的 leader broker 对象
	broker, err := client.Leader(topic, partitionID)
	if err != nil {
		return -1, err
	}

	// 构造请求
	request := &OffsetRequest{}
	if client.conf.Version.IsAtLeast(V0_10_1_0) {
		request.Version = 1
	}
	request.AddBlock(topic, partitionID, time, 1)

	// 发送请求
	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		_ = broker.Close()
		return -1, err
	}

	// 解析响应
	block := response.GetBlock(topic, partitionID)
	if block == nil {
		_ = broker.Close()
		return -1, ErrIncompleteResponse
	}
	if block.Err != ErrNoError {
		return -1, block.Err
	}
	if len(block.Offsets) != 1 {
		return -1, ErrOffsetOutOfRange
	}

	// 返回结果
	return block.Offsets[0], nil
}

// core metadata update logic

func (client *client) backgroundMetadataUpdater() {
	defer close(client.closed)


	// 按照配置的时间间隔更新元数据
	if client.conf.Metadata.RefreshFrequency == time.Duration(0) {
		return
	}

	ticker := time.NewTicker(client.conf.Metadata.RefreshFrequency)
	defer ticker.Stop()

	// 执行定时更新操作
	for {
		select {
		case <-ticker.C:
			if err := client.refreshMetadata(); err != nil {
				Logger.Println("Client background metadata update:", err)
			}
		case <-client.closer:
			return
		}
	}
}

// 判断需要刷新哪些主题的元数据，还是说要刷新全部主题的元数据。
func (client *client) refreshMetadata() error {

	var topics []string

	if !client.conf.Metadata.Full {
		if specificTopics, err := client.MetadataTopics(); err != nil {
			return err
		} else if len(specificTopics) == 0 {
			return ErrNoTopicsToUpdateMetadata
		} else {
			topics = specificTopics
		}
	}

	if err := client.RefreshMetadata(topics...); err != nil {
		return err
	}

	return nil
}


// 刷新本地缓存的 Metadata 元数据
func (client *client) tryRefreshMetadata(topics []string, attemptsRemaining int, deadline time.Time) error {

	// 过期检查
	pastDeadline := func(backoff time.Duration) bool {
		if !deadline.IsZero() && time.Now().Add(backoff).After(deadline) {
			// we are past the deadline
			return true
		}
		return false
	}

	// 重试机制
	// 当发生不需 retry 的 error 时，本函数直接 return ；
	// 当发生需要 retry 的 error 时，本函数调用 retry ，retry 里封装了重试的策略以及次数等。
	retry := func(err error) error {
		// 如果剩余重试次数大于 0 ，则继续重试
		if attemptsRemaining > 0 {
			// 执行退让算法
			backoff := client.computeBackoff(attemptsRemaining)
			if pastDeadline(backoff) {
				Logger.Println("client/metadata skipping last retries as we would go past the metadata timeout")
				return err
			}
			Logger.Printf("client/metadata retrying after %dms... (%d attempts remaining)\n", backoff/time.Millisecond, attemptsRemaining)
			// 等待
			if backoff > 0 {
				time.Sleep(backoff)
			}
			// [递归调用] 重试次数 -1 后进入重试
			return client.tryRefreshMetadata(topics, attemptsRemaining-1, deadline)
		}
		return err
	}

	// 选择一个可用的 broker
	broker := client.any()

	//
	for ; broker != nil && !pastDeadline(0); broker = client.any() {

		// 如果 topic 不存在，就自动创建
		allowAutoTopicCreation := true
		if len(topics) > 0 {
			Logger.Printf("client/metadata fetching metadata for %v from broker %s\n", topics, broker.addr)
		} else {
			// 如果未指定 topics 则拉取所有 topics 元数据，此时需要关闭 topic 的自动创建
			allowAutoTopicCreation = false
			Logger.Printf("client/metadata fetching metadata for all topics from broker %s\n", broker.addr)
		}

		// 构造获取元数据请求
		req := &MetadataRequest{
			Topics: topics,										// topics 列表
			AllowAutoTopicCreation: allowAutoTopicCreation,  	// 是否允许创建不存在的 topic
		}
		if client.conf.Version.IsAtLeast(V1_0_0_0) {
			req.Version = 5
		} else if client.conf.Version.IsAtLeast(V0_10_0_0) {
			req.Version = 1
		}

		// 执行请求，获取返回值
		response, err := broker.GetMetadata(req)
		switch err.(type) {
		// 请求成功，更新本地的元数据缓存
		case nil:
			allKnownMetaData := len(topics) == 0
			// valid response, use it
			// 更新元数据
			shouldRetry, err := client.updateMetadata(response, allKnownMetaData)
			if shouldRetry {
				Logger.Println("client/metadata found some partitions to be leaderless")
				return retry(err) // note: err can be nil
			}
			return err
		// 编码错误，不可重试，直接返回
		case PacketEncodingError:
			// didn't even send, return the error
			return err
		// 鉴权错误
		case KError:
			// if SASL auth error return as this _should_ be a non retryable err for all brokers
			if err.(KError) == ErrSASLAuthenticationFailed {
				Logger.Println("client/metadata failed SASL authentication")
				return err
			}
			if err.(KError) == ErrTopicAuthorizationFailed {
				Logger.Println("client is not authorized to access this topic. The topics were: ", topics)
				return err
			}
			// else remove that broker and try again
			Logger.Printf("client/metadata got error from broker %d while fetching metadata: %v\n", broker.ID(), err)
			_ = broker.Close()
			client.deregisterBroker(broker)
		// 其它错误，重试
		default:
			// some other error, remove that broker and try again
			Logger.Printf("client/metadata got error from broker %d while fetching metadata: %v\n", broker.ID(), err)
			_ = broker.Close()
			client.deregisterBroker(broker)
		}
	}

	if broker != nil {
		Logger.Printf("client/metadata not fetching metadata from broker %s as we would go past the metadata timeout\n", broker.addr)
		return retry(ErrOutOfBrokers)
	}

	Logger.Println("client/metadata no available broker to send metadata request to")
	client.resurrectDeadBrokers()
	return retry(ErrOutOfBrokers)
}

// if no fatal error, returns a list of topics that need retrying due to ErrLeaderNotAvailable
func (client *client) updateMetadata(data *MetadataResponse, allKnownMetaData bool) (retry bool, err error) {
	if client.Closed() {
		return
	}
	client.lock.Lock()
	defer client.lock.Unlock()

	// 1. 首先对本地缓存的 broker 元信息进行更新。

	// For all the brokers we received:
	// - if it is a new ID, save it
	// - if it is an existing ID, but the address we have is stale, discard the old one and save it
	// - if some brokers is not exist in it, remove old broker
	// - otherwise ignore it, replacing our existing one would just bounce the connection
	//
	//
	// 假设返回了新的 broker id ，那么保存这些新的 broker ，这意味着增加了 broker、或者下线的 broker 重新上线了。
	// 如果返回的 id 我们已经保存了，但是地址变化了，那么更新地址；
	// 如果本地保存的一些 id 没有返回，说明这些 broker 下线了，那么删除他们。
	//
	client.updateBroker(data.Brokers)

	client.controllerID = data.ControllerID

	if allKnownMetaData {
		client.metadata = make(map[string]map[int32]*PartitionMetadata)
		client.metadataTopics = make(map[string]none)
		client.cachedPartitionsResults = make(map[string][maxPartitionIndex][]int32)
	}

	// 2. 然后对本地缓存的 topic 元信息进行更新，主要是更新 topic 以及 topic 对应的 partition 。
	for _, topic := range data.Topics {


		// topics must be added firstly to `metadataTopics` to guarantee that all
		// requested topics must be recorded to keep them trackable for periodically
		// metadata refresh.
		if _, exists := client.metadataTopics[topic.Name]; !exists {
			client.metadataTopics[topic.Name] = none{}
		}
		delete(client.metadata, topic.Name)
		delete(client.cachedPartitionsResults, topic.Name)

		switch topic.Err {
		case ErrNoError:
			// no-op
		case ErrInvalidTopic, ErrTopicAuthorizationFailed: // don't retry, don't store partial results
			err = topic.Err
			continue
		case ErrUnknownTopicOrPartition: // retry, do not store partial partition results
			err = topic.Err
			retry = true
			continue
		case ErrLeaderNotAvailable: // retry, but store partial partition results
			retry = true
		default: // don't retry, don't store partial results
			Logger.Printf("Unexpected topic-level metadata error: %s", topic.Err)
			err = topic.Err
			continue
		}

		// 更新每个 topic 以及对应的 partition
		client.metadata[topic.Name] = make(map[int32]*PartitionMetadata, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			client.metadata[topic.Name][partition.ID] = partition
			if partition.Err == ErrLeaderNotAvailable {
				retry = true
			}
		}

		//
		var partitionCache [maxPartitionIndex][]int32
		partitionCache[allPartitions] = client.setPartitionCache(topic.Name, allPartitions)
		partitionCache[writablePartitions] = client.setPartitionCache(topic.Name, writablePartitions)
		client.cachedPartitionsResults[topic.Name] = partitionCache
	}

	return
}

func (client *client) cachedCoordinator(consumerGroup string) *Broker {
	client.lock.RLock()
	defer client.lock.RUnlock()
	if coordinatorID, ok := client.coordinators[consumerGroup]; ok {
		return client.brokers[coordinatorID]
	}
	return nil
}

func (client *client) cachedController() *Broker {
	client.lock.RLock()
	defer client.lock.RUnlock()

	return client.brokers[client.controllerID]
}

func (client *client) computeBackoff(attemptsRemaining int) time.Duration {
	if client.conf.Metadata.Retry.BackoffFunc != nil {
		maxRetries := client.conf.Metadata.Retry.Max
		retries := maxRetries - attemptsRemaining
		return client.conf.Metadata.Retry.BackoffFunc(retries, maxRetries)
	}
	return client.conf.Metadata.Retry.Backoff
}

func (client *client) getConsumerMetadata(consumerGroup string, attemptsRemaining int) (*FindCoordinatorResponse, error) {

	retry := func(err error) (*FindCoordinatorResponse, error) {
		if attemptsRemaining > 0 {
			backoff := client.computeBackoff(attemptsRemaining)
			Logger.Printf("client/coordinator retrying after %dms... (%d attempts remaining)\n", backoff/time.Millisecond, attemptsRemaining)
			time.Sleep(backoff)
			return client.getConsumerMetadata(consumerGroup, attemptsRemaining-1)
		}
		return nil, err
	}

	for broker := client.any(); broker != nil; broker = client.any() {
		Logger.Printf("client/coordinator requesting coordinator for consumergroup %s from %s\n", consumerGroup, broker.Addr())

		request := new(FindCoordinatorRequest)
		request.CoordinatorKey = consumerGroup
		request.CoordinatorType = CoordinatorGroup

		response, err := broker.FindCoordinator(request)

		if err != nil {
			Logger.Printf("client/coordinator request to broker %s failed: %s\n", broker.Addr(), err)

			switch err.(type) {
			case PacketEncodingError:
				return nil, err
			default:
				_ = broker.Close()
				client.deregisterBroker(broker)
				continue
			}
		}

		switch response.Err {
		case ErrNoError:
			Logger.Printf("client/coordinator coordinator for consumergroup %s is #%d (%s)\n", consumerGroup, response.Coordinator.ID(), response.Coordinator.Addr())
			return response, nil

		case ErrConsumerCoordinatorNotAvailable:
			Logger.Printf("client/coordinator coordinator for consumer group %s is not available\n", consumerGroup)

			// This is very ugly, but this scenario will only happen once per cluster.
			// The __consumer_offsets topic only has to be created one time.
			// The number of partitions not configurable, but partition 0 should always exist.
			if _, err := client.Leader("__consumer_offsets", 0); err != nil {
				Logger.Printf("client/coordinator the __consumer_offsets topic is not initialized completely yet. Waiting 2 seconds...\n")
				time.Sleep(2 * time.Second)
			}

			return retry(ErrConsumerCoordinatorNotAvailable)
		case ErrGroupAuthorizationFailed:
			Logger.Printf("client was not authorized to access group %s while attempting to find coordinator", consumerGroup)
			return retry(ErrGroupAuthorizationFailed)

		default:
			return nil, response.Err
		}
	}

	Logger.Println("client/coordinator no available broker to send consumer metadata request to")
	client.resurrectDeadBrokers()
	return retry(ErrOutOfBrokers)
}

// nopCloserClient embeds an existing Client, but disables
// the Close method (yet all other methods pass
// through unchanged). This is for use in larger structs
// where it is undesirable to close the client that was
// passed in by the caller.
type nopCloserClient struct {
	Client
}

// Close intercepts and purposely does not call the underlying
// client's Close() method.
func (ncc *nopCloserClient) Close() error {
	return nil
}
