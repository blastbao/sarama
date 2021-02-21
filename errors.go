package sarama

import (
	"errors"
	"fmt"
)

// ErrOutOfBrokers is the error returned when the client has run out of brokers to talk to because all of them errored
// or otherwise failed to respond.
var ErrOutOfBrokers = errors.New("kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")

// ErrBrokerNotFound is the error returned when there's no broker found for the requested ID.
var ErrBrokerNotFound = errors.New("kafka: broker for ID is not found")

// ErrClosedClient is the error returned when a method is called on a client that has been closed.
var ErrClosedClient = errors.New("kafka: tried to use a client that was closed")

// ErrIncompleteResponse is the error returned when the server returns a syntactically valid response, but it does
// not contain the expected information.
var ErrIncompleteResponse = errors.New("kafka: response did not contain all the expected topic/partition blocks")

// ErrInvalidPartition is the error returned when a partitioner returns an invalid partition index
// (meaning one outside of the range [0...numPartitions-1]).
var ErrInvalidPartition = errors.New("kafka: partitioner returned an invalid partition index")

// ErrAlreadyConnected is the error returned when calling Open() on a Broker that is already connected or connecting.
var ErrAlreadyConnected = errors.New("kafka: broker connection already initiated")

// ErrNotConnected is the error returned when trying to send or call Close() on a Broker that is not connected.
var ErrNotConnected = errors.New("kafka: broker not connected")

// ErrInsufficientData is returned when decoding and the packet is truncated. This can be expected
// when requesting messages, since as an optimization the server is allowed to return a partial message at the end
// of the message set.
var ErrInsufficientData = errors.New("kafka: insufficient data to decode packet, more bytes expected")

// ErrShuttingDown is returned when a producer receives a message during shutdown.
var ErrShuttingDown = errors.New("kafka: message received by producer in process of shutting down")

// ErrMessageTooLarge is returned when the next message to consume is larger than the configured Consumer.Fetch.Max
var ErrMessageTooLarge = errors.New("kafka: message is larger than Consumer.Fetch.Max")

// ErrConsumerOffsetNotAdvanced is returned when a partition consumer didn't advance its offset after parsing
// a RecordBatch.
var ErrConsumerOffsetNotAdvanced = errors.New("kafka: consumer offset was not advanced after a RecordBatch")

// ErrControllerNotAvailable is returned when server didn't give correct controller id. May be kafka server's version
// is lower than 0.10.0.0.
var ErrControllerNotAvailable = errors.New("kafka: controller is not available")

// ErrNoTopicsToUpdateMetadata is returned when Meta.Full is set to false but no specific topics were found to update
// the metadata.
var ErrNoTopicsToUpdateMetadata = errors.New("kafka: no specific topics to update metadata")

// PacketEncodingError is returned from a failure while encoding a Kafka packet. This can happen, for example,
// if you try to encode a string over 2^15 characters in length, since Kafka's encoding rules do not permit that.
type PacketEncodingError struct {
	Info string
}

func (err PacketEncodingError) Error() string {
	return fmt.Sprintf("kafka: error encoding packet: %s", err.Info)
}

// PacketDecodingError is returned when there was an error (other than truncated data) decoding the Kafka broker's response.
// This can be a bad CRC or length field, or any other invalid value.
type PacketDecodingError struct {
	Info string
}

func (err PacketDecodingError) Error() string {
	return fmt.Sprintf("kafka: error decoding packet: %s", err.Info)
}

// ConfigurationError is the type of error returned from a constructor (e.g. NewClient, or NewConsumer)
// when the specified configuration is invalid.
type ConfigurationError string

func (err ConfigurationError) Error() string {
	return "kafka: invalid configuration (" + string(err) + ")"
}

// KError is the type of error that can be returned directly by the Kafka broker.
// See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
type KError int16

// MultiError is used to contain multi error.
type MultiError struct {
	Errors *[]error
}

func (mErr MultiError) Error() string {
	var errString = ""
	for _, err := range *mErr.Errors {
		errString += err.Error() + ","
	}
	return errString
}

func (mErr MultiError) PrettyError() string {
	var errString = ""
	for _, err := range *mErr.Errors {
		errString += err.Error() + "\n"
	}
	return errString
}

// ErrDeleteRecords is the type of error returned when fail to delete the required records
type ErrDeleteRecords struct {
	MultiError
}

func (err ErrDeleteRecords) Error() string {
	return "kafka server: failed to delete records " + err.MultiError.Error()
}

type ErrReassignPartitions struct {
	MultiError
}

func (err ErrReassignPartitions) Error() string {
	return fmt.Sprintf("failed to reassign partitions for topic: \n%s", err.MultiError.PrettyError())
}

// Numeric error codes returned by the Kafka server.
const (
	ErrNoError                            KError = 0
	ErrUnknown                            KError = -1

	// 报错原因：当消费者消费 offset 大于或小于当前 kafka 集群的 offset 值时，消费会报错。
	// 比如一个 consumer group 消费某 topic ，当 consumer group 间隔几天不消费，Kafka 内部数据会自动清除之前的数据，
	// 程序再次启动时，会找之前消费到的 offset 进行消费，此时，若 Kafka 已经删除此 offset 值，就会产生此报错。
	//
	// 解决办法：换个 groupid 进行消费 。
	ErrOffsetOutOfRange                   KError = 1

	ErrInvalidMessage                     KError = 2

	// 报错内容：分区不存在
	// 原因分析：producer 向不存在的 topic 发送消息，用户可以检查 topic 是否存在或者设置 auto.create.topics.enable 参数。
	ErrUnknownTopicOrPartition            KError = 3

	// 报错内容：消息过大
	// 原因分析：生产者端消息处理不过来了，可以增加 request.timeout.ms 减少 batch.size 。
	ErrInvalidMessageSize                 KError = 4

	// The error message shows you don't have a valid leader for the partition you are accessing.
	// In kafka, all read/writes should go through the leader of that partition.
	//
	// You should make sure the topic/partitions have healthy leader first, run:
	// kafka-topics --describe --zookeeper <zk_url, put /chroot if you have any> --topic <topic_name>
	//
	// 报错内容：leader不可用
	// 原因分析：原因很多，topic 正在被删除、正在进行leader选举...
	// 解决方法：可使用 kafka-topics 脚本检查 leader 信息，进而检查 broker 的存活情况，尝试重启解决。
	ErrLeaderNotAvailable                 KError = 5

	// Each topic is served by one or multiple Brokers - one is leader and the remaining brokers are followers.
	// A producer needs to send new messages to the leader Broker which internally replicate the data to all followers.
	// I assume, that your producer client does not connect to the correct Broker,
	// its connect to a follower instead of the leader, and this follower rejects your send request.
	//
	// Try to run:
	// 	./kafka-topics.sh --zookeeper localhost:2181 --topic your_topic --describe
	//
	// Output:
	// 	Topic: your_topic   PartitionCount:3    ReplicationFactor:1 Configs:retention.ms=14400000
	//	Topic: your_topic   Partition: 0    	Leader: 2  			Replicas: 2 Isr: 2
	//	Topic: your_topic   Partition: 1    	Leader: 0   		Replicas: 0 Isr: 0
	//	Topic: your_topic   Partition: 2    	Leader: 1   		Replicas: 1 Isr: 1
	//
	// In this example you can see that your_topic have 3 partitions meaning all 3 brokers are leaders
	// of that topic each on different partition,
	// s.t broker 2 is leader on partition 0 and broker 0 and broker 1 are followers on partition 0.
	//
	// Generally, with retry > 0 producer should be able to find the correct leader in the subsequent retries.
	//
	//
	// 报错内容：broker 已经不是对应 partition 的 leader 了
	// 原因分析：发生在 leader 变更时，当 leader 从一个 broker 切换到另一个 broker 时，要分析什么原因引起了 leader 的切换。
	ErrNotLeaderForPartition              KError = 6

	// 报错内容：请求超时
	// 原因分析：观察哪里抛出的，观察网络是否能通，如果可以通，可以考虑增加 request.timeout.ms 的值。
	ErrRequestTimedOut                    KError = 7

	//
	ErrBrokerNotAvailable                 KError = 8
	ErrReplicaNotAvailable                KError = 9
	ErrMessageSizeTooLarge                KError = 10
	ErrStaleControllerEpochCode           KError = 11
	ErrOffsetMetadataTooLarge             KError = 12

	// 报错内容：网络异常
	// 原因分析：网络连接中断，检查 broker 的网络情况
	ErrNetworkException                   KError = 13
	ErrOffsetsLoadInProgress              KError = 14
	ErrConsumerCoordinatorNotAvailable    KError = 15
	ErrNotCoordinatorForConsumer          KError = 16
	ErrInvalidTopic                       KError = 17
	ErrMessageSetSizeTooLarge             KError = 18
	ErrNotEnoughReplicas                  KError = 19
	ErrNotEnoughReplicasAfterAppend       KError = 20
	ErrInvalidRequiredAcks                KError = 21

	// 报错内容：无效的“代”
	// 原因分析：consumer 错过了 rebalance ，原因是 consumer 花了大量时间处理数据。
	// 需要适当减少 max.poll.records 值，增加 max.poll.interval.ms 或者想办法增加消息处理的速度。
	ErrIllegalGeneration                  KError = 22

	ErrInconsistentGroupProtocol          KError = 23
	ErrInvalidGroupId                     KError = 24
	ErrUnknownMemberId                    KError = 25
	ErrInvalidSessionTimeout              KError = 26
	ErrRebalanceInProgress                KError = 27
	ErrInvalidCommitOffsetSize            KError = 28
	ErrTopicAuthorizationFailed           KError = 29
	ErrGroupAuthorizationFailed           KError = 30
	ErrClusterAuthorizationFailed         KError = 31
	ErrInvalidTimestamp                   KError = 32
	ErrUnsupportedSASLMechanism           KError = 33
	ErrIllegalSASLState                   KError = 34
	ErrUnsupportedVersion                 KError = 35
	ErrTopicAlreadyExists                 KError = 36
	ErrInvalidPartitions                  KError = 37
	ErrInvalidReplicationFactor           KError = 38
	ErrInvalidReplicaAssignment           KError = 39
	ErrInvalidConfig                      KError = 40
	ErrNotController                      KError = 41
	ErrInvalidRequest                     KError = 42
	ErrUnsupportedForMessageFormat        KError = 43
	ErrPolicyViolation                    KError = 44
	ErrOutOfOrderSequenceNumber           KError = 45
	ErrDuplicateSequenceNumber            KError = 46
	ErrInvalidProducerEpoch               KError = 47
	ErrInvalidTxnState                    KError = 48
	ErrInvalidProducerIDMapping           KError = 49
	ErrInvalidTransactionTimeout          KError = 50
	ErrConcurrentTransactions             KError = 51
	ErrTransactionCoordinatorFenced       KError = 52
	ErrTransactionalIDAuthorizationFailed KError = 53
	ErrSecurityDisabled                   KError = 54
	ErrOperationNotAttempted              KError = 55
	ErrKafkaStorageError                  KError = 56
	ErrLogDirNotFound                     KError = 57
	ErrSASLAuthenticationFailed           KError = 58
	ErrUnknownProducerID                  KError = 59
	ErrReassignmentInProgress             KError = 60
	ErrDelegationTokenAuthDisabled        KError = 61
	ErrDelegationTokenNotFound            KError = 62
	ErrDelegationTokenOwnerMismatch       KError = 63
	ErrDelegationTokenRequestNotAllowed   KError = 64
	ErrDelegationTokenAuthorizationFailed KError = 65
	ErrDelegationTokenExpired             KError = 66
	ErrInvalidPrincipalType               KError = 67
	ErrNonEmptyGroup                      KError = 68
	ErrGroupIDNotFound                    KError = 69
	ErrFetchSessionIDNotFound             KError = 70
	ErrInvalidFetchSessionEpoch           KError = 71
	ErrListenerNotFound                   KError = 72
	ErrTopicDeletionDisabled              KError = 73
	ErrFencedLeaderEpoch                  KError = 74
	ErrUnknownLeaderEpoch                 KError = 75
	ErrUnsupportedCompressionType         KError = 76
	ErrStaleBrokerEpoch                   KError = 77
	ErrOffsetNotAvailable                 KError = 78
	ErrMemberIdRequired                   KError = 79
	ErrPreferredLeaderNotAvailable        KError = 80
	ErrGroupMaxSizeReached                KError = 81
	ErrFencedInstancedId                  KError = 82
)

func (err KError) Error() string {
	// Error messages stolen/adapted from
	// https://kafka.apache.org/protocol#protocol_error_codes
	switch err {
	case ErrNoError:
		return "kafka server: Not an error, why are you printing me?"
	case ErrUnknown:
		return "kafka server: Unexpected (unknown?) server error."
	case ErrOffsetOutOfRange:
		return "kafka server: The requested offset is outside the range of offsets maintained by the server for the given topic/partition."
	case ErrInvalidMessage:
		return "kafka server: Message contents does not match its CRC."
	case ErrUnknownTopicOrPartition:
		return "kafka server: Request was for a topic or partition that does not exist on this broker."
	case ErrInvalidMessageSize:
		return "kafka server: The message has a negative size."
	case ErrLeaderNotAvailable:
		return "kafka server: In the middle of a leadership election, there is currently no leader for this partition and hence it is unavailable for writes."
	case ErrNotLeaderForPartition:
		return "kafka server: Tried to send a message to a replica that is not the leader for some partition. Your metadata is out of date."
	case ErrRequestTimedOut:
		return "kafka server: Request exceeded the user-specified time limit in the request."
	case ErrBrokerNotAvailable:
		return "kafka server: Broker not available. Not a client facing error, we should never receive this!!!"
	case ErrReplicaNotAvailable:
		return "kafka server: Replica information not available, one or more brokers are down."
	case ErrMessageSizeTooLarge:
		return "kafka server: Message was too large, server rejected it to avoid allocation error."
	case ErrStaleControllerEpochCode:
		return "kafka server: StaleControllerEpochCode (internal error code for broker-to-broker communication)."
	case ErrOffsetMetadataTooLarge:
		return "kafka server: Specified a string larger than the configured maximum for offset metadata."
	case ErrNetworkException:
		return "kafka server: The server disconnected before a response was received."
	case ErrOffsetsLoadInProgress:
		return "kafka server: The broker is still loading offsets after a leader change for that offset's topic partition."
	case ErrConsumerCoordinatorNotAvailable:
		return "kafka server: Offset's topic has not yet been created."
	case ErrNotCoordinatorForConsumer:
		return "kafka server: Request was for a consumer group that is not coordinated by this broker."
	case ErrInvalidTopic:
		return "kafka server: The request attempted to perform an operation on an invalid topic."
	case ErrMessageSetSizeTooLarge:
		return "kafka server: The request included message batch larger than the configured segment size on the server."
	case ErrNotEnoughReplicas:
		return "kafka server: Messages are rejected since there are fewer in-sync replicas than required."
	case ErrNotEnoughReplicasAfterAppend:
		return "kafka server: Messages are written to the log, but to fewer in-sync replicas than required."
	case ErrInvalidRequiredAcks:
		return "kafka server: The number of required acks is invalid (should be either -1, 0, or 1)."
	case ErrIllegalGeneration:
		return "kafka server: The provided generation id is not the current generation."
	case ErrInconsistentGroupProtocol:
		return "kafka server: The provider group protocol type is incompatible with the other members."
	case ErrInvalidGroupId:
		return "kafka server: The provided group id was empty."
	case ErrUnknownMemberId:
		return "kafka server: The provided member is not known in the current generation."
	case ErrInvalidSessionTimeout:
		return "kafka server: The provided session timeout is outside the allowed range."
	case ErrRebalanceInProgress:
		return "kafka server: A rebalance for the group is in progress. Please re-join the group."
	case ErrInvalidCommitOffsetSize:
		return "kafka server: The provided commit metadata was too large."
	case ErrTopicAuthorizationFailed:
		return "kafka server: The client is not authorized to access this topic."
	case ErrGroupAuthorizationFailed:
		return "kafka server: The client is not authorized to access this group."
	case ErrClusterAuthorizationFailed:
		return "kafka server: The client is not authorized to send this request type."
	case ErrInvalidTimestamp:
		return "kafka server: The timestamp of the message is out of acceptable range."
	case ErrUnsupportedSASLMechanism:
		return "kafka server: The broker does not support the requested SASL mechanism."
	case ErrIllegalSASLState:
		return "kafka server: Request is not valid given the current SASL state."
	case ErrUnsupportedVersion:
		return "kafka server: The version of API is not supported."
	case ErrTopicAlreadyExists:
		return "kafka server: Topic with this name already exists."
	case ErrInvalidPartitions:
		return "kafka server: Number of partitions is invalid."
	case ErrInvalidReplicationFactor:
		return "kafka server: Replication-factor is invalid."
	case ErrInvalidReplicaAssignment:
		return "kafka server: Replica assignment is invalid."
	case ErrInvalidConfig:
		return "kafka server: Configuration is invalid."
	case ErrNotController:
		return "kafka server: This is not the correct controller for this cluster."
	case ErrInvalidRequest:
		return "kafka server: This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."
	case ErrUnsupportedForMessageFormat:
		return "kafka server: The requested operation is not supported by the message format version."
	case ErrPolicyViolation:
		return "kafka server: Request parameters do not satisfy the configured policy."
	case ErrOutOfOrderSequenceNumber:
		return "kafka server: The broker received an out of order sequence number."
	case ErrDuplicateSequenceNumber:
		return "kafka server: The broker received a duplicate sequence number."
	case ErrInvalidProducerEpoch:
		return "kafka server: Producer attempted an operation with an old epoch."
	case ErrInvalidTxnState:
		return "kafka server: The producer attempted a transactional operation in an invalid state."
	case ErrInvalidProducerIDMapping:
		return "kafka server: The producer attempted to use a producer id which is not currently assigned to its transactional id."
	case ErrInvalidTransactionTimeout:
		return "kafka server: The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms)."
	case ErrConcurrentTransactions:
		return "kafka server: The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing."
	case ErrTransactionCoordinatorFenced:
		return "kafka server: The transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer."
	case ErrTransactionalIDAuthorizationFailed:
		return "kafka server: Transactional ID authorization failed."
	case ErrSecurityDisabled:
		return "kafka server: Security features are disabled."
	case ErrOperationNotAttempted:
		return "kafka server: The broker did not attempt to execute this operation."
	case ErrKafkaStorageError:
		return "kafka server: Disk error when trying to access log file on the disk."
	case ErrLogDirNotFound:
		return "kafka server: The specified log directory is not found in the broker config."
	case ErrSASLAuthenticationFailed:
		return "kafka server: SASL Authentication failed."
	case ErrUnknownProducerID:
		return "kafka server: The broker could not locate the producer metadata associated with the Producer ID."
	case ErrReassignmentInProgress:
		return "kafka server: A partition reassignment is in progress."
	case ErrDelegationTokenAuthDisabled:
		return "kafka server: Delegation Token feature is not enabled."
	case ErrDelegationTokenNotFound:
		return "kafka server: Delegation Token is not found on server."
	case ErrDelegationTokenOwnerMismatch:
		return "kafka server: Specified Principal is not valid Owner/Renewer."
	case ErrDelegationTokenRequestNotAllowed:
		return "kafka server: Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels."
	case ErrDelegationTokenAuthorizationFailed:
		return "kafka server: Delegation Token authorization failed."
	case ErrDelegationTokenExpired:
		return "kafka server: Delegation Token is expired."
	case ErrInvalidPrincipalType:
		return "kafka server: Supplied principalType is not supported."
	case ErrNonEmptyGroup:
		return "kafka server: The group is not empty."
	case ErrGroupIDNotFound:
		return "kafka server: The group id does not exist."
	case ErrFetchSessionIDNotFound:
		return "kafka server: The fetch session ID was not found."
	case ErrInvalidFetchSessionEpoch:
		return "kafka server: The fetch session epoch is invalid."
	case ErrListenerNotFound:
		return "kafka server: There is no listener on the leader broker that matches the listener on which metadata request was processed."
	case ErrTopicDeletionDisabled:
		return "kafka server: Topic deletion is disabled."
	case ErrFencedLeaderEpoch:
		return "kafka server: The leader epoch in the request is older than the epoch on the broker."
	case ErrUnknownLeaderEpoch:
		return "kafka server: The leader epoch in the request is newer than the epoch on the broker."
	case ErrUnsupportedCompressionType:
		return "kafka server: The requesting client does not support the compression type of given partition."
	case ErrStaleBrokerEpoch:
		return "kafka server: Broker epoch has changed"
	case ErrOffsetNotAvailable:
		return "kafka server: The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing"
	case ErrMemberIdRequired:
		return "kafka server: The group member needs to have a valid member id before actually entering a consumer group"
	case ErrPreferredLeaderNotAvailable:
		return "kafka server: The preferred leader was not available"
	case ErrGroupMaxSizeReached:
		return "kafka server: Consumer group The consumer group has reached its max size. already has the configured maximum number of members."
	case ErrFencedInstancedId:
		return "kafka server: The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id."
	}

	return fmt.Sprintf("Unknown error, how did this happen? Error code = %d", err)
}
