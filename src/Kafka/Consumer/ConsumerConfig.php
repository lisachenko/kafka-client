<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

/**
 * @author Alexander.Lisachenko
 * @date   01.08.2016
 */

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\Common\ClientConfig as GeneralConfig;
use Protocol\Kafka\Protocol\Request\FetchRequest;

/**
 * Consumer config enumeration class
 *
 * Kafka 0.9 brought the broker-side group management, so the options that drive it exist here: session.timeout.ms,
 * heartbeat.interval.ms and partition.assignment.strategy, plus offset.retention.ms for the `retention_time` of the
 * OffsetCommit v2 request. Kafka 0.10.1 added {@see self::MAX_POLL_INTERVAL_MS}, the application-side half of the
 * `rebalance_timeout` that a JoinGroup v1 request carries, and {@see self::FETCH_MAX_BYTES}, the request-level
 * bound of a Fetch v3 answer. Kafka 0.11 added {@see self::ISOLATION_LEVEL}, the option of the transactional
 * protocol of KIP-98, which decides whether a consumer sees the records of a transaction that is still open or
 * that was aborted; it travels in the Fetch v5 request and in the Offsets v2 request alike. The committed offsets
 * always live in the `__consumer_offsets` topic of the cluster: the ZooKeeper storage of Kafka 0.8.1 (the version 0
 * of the offset apis, the former `offsets.storage` option) is gone from this line, because a KRaft node answers
 * both v0 requests with the error code 35.
 *
 * A consumer overrides one option of the general config: `request.timeout.ms` defaults to 305000 instead of 30000,
 * as it does in the Java consumer of 0.10.1 and above ("chosen to be higher than the default of
 * max.poll.interval.ms", `ConsumerConfig.java` @ 0.10.2.2), because it has to be larger than both
 * `session.timeout.ms` and `max.poll.interval.ms`: the socket would otherwise time out on a JoinGroup that the
 * coordinator holds until the rebalance of the group is over, and a rebalance may last a whole rebalance timeout.
 */
final class ConsumerConfig extends GeneralConfig
{
    /**
     * Default configuration for consumer
     */
    private static array $consumerConfiguration = [
        /* Used configs */
        ConsumerConfig::GROUP_ID                      => '',
        ConsumerConfig::GROUP_INSTANCE_ID             => null,
        ConsumerConfig::GROUP_PROTOCOL                => ConsumerConfig::GROUP_PROTOCOL_CLASSIC,
        ConsumerConfig::GROUP_REMOTE_ASSIGNOR         => null,
        ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => 'range',
        // Larger than both timeouts below, as in the Java consumer of 0.10.1 and above: the coordinator answers a
        // JoinGroup only once the whole rebalance is over, which can take a full rebalance timeout
        ConsumerConfig::REQUEST_TIMEOUT_MS            => 305000,
        ConsumerConfig::SESSION_TIMEOUT_MS            => 10000,
        ConsumerConfig::MAX_POLL_INTERVAL_MS          => ConsumerConfig::DEFAULT_MAX_POLL_INTERVAL_MS,
        ConsumerConfig::HEARTBEAT_INTERVAL_MS         => 3000,
        ConsumerConfig::FETCH_MIN_BYTES               => 1,
        ConsumerConfig::FETCH_MAX_BYTES               => 52428800,
        ConsumerConfig::FETCH_MAX_WAIT_MS             => 500,
        ConsumerConfig::MAX_PARTITION_FETCH_BYTES     => 65536,
        ConsumerConfig::AUTO_OFFSET_RESET             => OffsetResetStrategy::LATEST,
        ConsumerConfig::ENABLE_AUTO_COMMIT            => true,
        ConsumerConfig::AUTO_COMMIT_INTERVAL_MS       => 0, // Commit always after each poll()
        ConsumerConfig::OFFSET_RETENTION_MS           => -1, // Use the broker retention time for offsets
        ConsumerConfig::CHECK_CRCS                    => true,
        ConsumerConfig::ISOLATION_LEVEL               => ConsumerConfig::ISOLATION_LEVEL_READ_UNCOMMITTED,
        ConsumerConfig::CLIENT_RACK                   => FetchRequest::NO_RACK,
        ConsumerConfig::KEY_DESERIALIZER              => null,
        ConsumerConfig::VALUE_DESERIALIZER            => null,
        // KIP-932: read by the share consumer alone, as `ConsumerConfig` @ 4.3.1 defines them for every consumer
        ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE    => ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_IMPLICIT,
        ConsumerConfig::SHARE_ACQUIRE_MODE            => ConsumerConfig::SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED,
    ];

    /**
     * A unique string that identifies the consumer group this consumer belongs to.
     *
     * This property is required if the consumer uses either the group management functionality by using
     * subscribe(topic) or the Kafka-based offset management strategy.
     */
    public const string GROUP_ID = 'group.id';

    /**
     * A unique identifier of the consumer instance provided by the end user (`group.instance.id`, KIP-345)
     *
     * A consumer that carries one is a **static** member of its group: the coordinator remembers the member id
     * behind the instance id, so a consumer that restarts within its session timeout joins the group under the
     * very same identity and keeps the partitions it had, without a rebalance and without a new generation. A
     * consumer that leaves it unset (null) is a dynamic member, which is what every consumer of the lines below
     * Kafka 2.3 is.
     *
     * Two live consumers must never share one instance id: the second one to join takes the identity over and
     * every request of the first is answered 82 (`FencedInstanceId`) from then on, which this client reports as
     * {@see \Protocol\Kafka\Common\Errors\FencedInstanceIdException} and does not recover from.
     *
     * The value travels in the `group_instance_id` of JoinGroup v5, SyncGroup v3, Heartbeat v3 and
     * OffsetCommit v7 (Kafka 2.3), which are the versions this client sends.
     */
    public const string GROUP_INSTANCE_ID = 'group.instance.id';

    /**
     * Membership protocol this consumer speaks: `classic` (the default) or `consumer` (KIP-848, Kafka 3.5)
     *
     * `classic` is the protocol of every line below this one - JoinGroup, SyncGroup, Heartbeat and LeaveGroup,
     * with the assignment computed by the **leader of the group**, i.e. by one of the consumers.
     * {@see self::GROUP_PROTOCOL_CONSUMER} is the new consumer protocol of KIP-848, where those four apis are
     * replaced by the single ConsumerGroupHeartbeat (key 68) and the assignment is computed by the **coordinator**:
     *
     * * the heartbeat interval is dictated by the broker (`group.consumer.heartbeat.interval.ms`) and
     *   `heartbeat.interval.ms` of this configuration is not sent anywhere;
     * * {@see self::PARTITION_ASSIGNMENT_STRATEGY} is not used at all - there is no client-side assignor on this
     *   path - and {@see self::GROUP_REMOTE_ASSIGNOR} names the server-side one instead;
     * * a rebalance is **incremental**: a member gives up only the partitions it really loses, and the listener
     *   of {@see ConsumerRebalanceListener} sees exactly those, never the whole assignment;
     * * the `session.timeout.ms` of the client is not sent either - `group.consumer.session.timeout.ms` of the
     *   broker holds for every member of such a group.
     *
     * A group is of one protocol or the other, never of both: a classic JoinGroup for a group of the type
     * `consumer` is refused, and a ConsumerGroupHeartbeat for a classic group as well.
     *
     * @see \Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator
     * @see docs/protocol/4.3.md, section "ConsumerGroupHeartbeat API (key 68, v0 and v1)"
     */
    public const string GROUP_PROTOCOL = 'group.protocol';

    /**
     * Server-side assignor a `group.protocol=consumer` member asks the coordinator for (KIP-848)
     *
     * The `group.remote.assignor` of the Java consumer: the name of one of the assignors the broker offers in
     * `group.consumer.assignors`, `uniform` and `range` on a 3.9.2 node. The default **null** names none, which
     * lets the coordinator take the first of its list; a name the broker does not have is answered **112**
     * `UnsupportedAssignor`, which is fatal for the configuration and never retried.
     *
     * Every member of a group has to ask for the same assignor, exactly as every member of a classic group has to
     * offer the same {@see self::PARTITION_ASSIGNMENT_STRATEGY}.
     */
    public const string GROUP_REMOTE_ASSIGNOR = 'group.remote.assignor';

    /**
     * `group.protocol` of a consumer that speaks the classic membership protocol, the default
     */
    public const string GROUP_PROTOCOL_CLASSIC = 'classic';

    /**
     * `group.protocol` of a consumer that speaks the new consumer protocol of KIP-848
     */
    public const string GROUP_PROTOCOL_CONSUMER = 'consumer';

    /**
     * The partition assignment strategy that the client will use to distribute partition ownership amongst consumer
     * instances when group management is used.
     *
     * Either the wire name of a built-in assignor - `range` (the default, as in the Java client of 0.9) or
     * `roundrobin` - or the name of a class that implements {@see PartitionAssignorInterface}. The name is what the
     * JoinGroup request advertises to the coordinator as the group protocol, so every member of a group has to use
     * the same one (error 23 InconsistentGroupProtocol otherwise).
     */
    public const string PARTITION_ASSIGNMENT_STRATEGY = 'partition.assignment.strategy';

    /**
     * The timeout used to detect failures when using Kafka's group management facilities.
     *
     * When a consumer's heartbeat is not received within the session timeout, the broker will mark the consumer as
     * failed and rebalance the group.
     *
     * Since heartbeats are sent only when poll() is invoked, a higher session timeout allows more time for message
     * processing in the consumer's poll loop at the cost of a longer time to detect hard failures. See also
     * max.poll.records for another option to control the processing time in the poll loop. Note that the value must be
     * in the allowable range as configured in the broker configuration by group.min.session.timeout.ms and
     * group.max.session.timeout.ms (error 26 InvalidSessionTimeout otherwise).
     *
     * The default is 10000, which is the default of the Java consumer since Kafka 0.10.1: what an application may
     * spend between two poll() calls is bounded by {@see self::MAX_POLL_INTERVAL_MS} from then on, so the session
     * timeout no longer has to cover the processing of a whole batch and may be short enough to notice a member that
     * died quickly.
     */
    public const string SESSION_TIMEOUT_MS = 'session.timeout.ms';

    /**
     * The maximum delay between two invocations of poll() for a consumer that uses the group management.
     *
     * The value is sent to the coordinator as the `rebalance_timeout` of a JoinGroup v1 request (Kafka 0.10.1,
     * KIP-62), which is how long the coordinator waits for **this** member to rejoin a rebalance before it hands its
     * partitions to somebody else. The Java consumer also leaves the group by itself when the application does not
     * come back to poll() within this interval, which it can do because its heartbeats are sent from a thread of
     * their own.
     *
     * **This client has no such thread.** PHP is single threaded and the heartbeat is sent from poll(), as it is on
     * the 0.9 line, so an application that stops polling stops sending heartbeats as well: the coordinator drops the
     * member when its `session.timeout.ms` expires - not when this interval does - and the next poll() sees the
     * error code 25 (UnknownMemberId) or 27 (RebalanceInProgress) on its heartbeat and joins the group again. What
     * this option really controls here is therefore how long the *other* members of the group wait in a rebalance,
     * and for how long a JoinGroup of this client may block.
     *
     * Because a JoinGroup blocks for up to this long, `request.timeout.ms` has to be larger than it, which is what
     * {@see KafkaConsumer::subscribe()} refuses a configuration over.
     */
    public const string MAX_POLL_INTERVAL_MS = 'max.poll.interval.ms';

    /**
     * Default of {@see self::MAX_POLL_INTERVAL_MS}: five minutes, as in the Java consumer of 0.10.1 and above
     */
    public const int DEFAULT_MAX_POLL_INTERVAL_MS = 300000;

    /**
     * The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities.
     *
     * Heartbeats are used to ensure that the consumer's session stays active and to facilitate rebalancing when new
     * consumers join or leave the group. The value must be set lower than session.timeout.ms, but typically should be
     * set no higher than 1/3 of that value. It can be adjusted even lower to control the expected time for normal
     * rebalances. PHP has no background thread: the heartbeat is sent from poll() once this interval has elapsed.
     */
    public const string HEARTBEAT_INTERVAL_MS = 'heartbeat.interval.ms';

    /**
     * The minimum amount of data the server should return for a fetch request.
     *
     * If insufficient data is available the request will wait for that much data to accumulate before answering the
     * request. The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data
     * is available or the fetch request times out waiting for data to arrive. Setting this to something greater than 1
     * will cause the server to wait for larger amounts of data to accumulate which can improve server throughput a bit
     * at the cost of some additional latency.
     */
    public const string FETCH_MIN_BYTES = 'fetch.min.bytes';

    /**
     * The maximum amount of data the server should return for a fetch request, over all of its partitions.
     *
     * It is the request-level `MaxBytes` that version 3 of the Fetch API added (Kafka 0.10.1, KIP-74), 50 MiB by
     * default as in the Java consumer. The broker fills the partitions of a request in the order they were asked
     * for and stops once this budget is used up, so a consumer that fetches many partitions has to rotate their
     * order to be fair - {@see KafkaConsumer::poll()} does exactly that.
     *
     * The limit is not absolute: if the first message of the first non-empty partition is larger than this value,
     * it is returned anyway, so that the consumer can always make progress. `max.partition.fetch.bytes` keeps its
     * own, per-partition meaning next to it.
     *
     * @see \Protocol\Kafka\Protocol\Request\FetchRequest::$maxBytes
     */
    public const string FETCH_MAX_BYTES = 'fetch.max.bytes';

    /**
     * The maximum amount of time the server will block before answering the fetch request if there isn't sufficient
     * data to immediately satisfy the requirement given by fetch.min.bytes.
     */
    public const string FETCH_MAX_WAIT_MS = 'fetch.max.wait.ms';

    /**
     * The maximum amount of data per-partition the server will return.
     *
     * This size must be at least as large as the maximum message size the server allows or else it is possible for the
     * producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying
     * to fetch a large message on a certain partition.
     */
    public const string MAX_PARTITION_FETCH_BYTES = 'max.partition.fetch.bytes';

    /**
     * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the
     * server (e.g. because that data has been deleted):
     *
     * earliest: automatically reset the offset to the earliest offset
     * latest: automatically reset the offset to the latest offset
     * none: throw exception to the consumer if no previous offset is found for the consumer's group
     * anything else: throw exception to the consumer.
     */
    public const string AUTO_OFFSET_RESET = 'auto.offset.reset';

    /**
     * If true the consumer's offset will be periodically committed after poll() operation.
     */
    public const string ENABLE_AUTO_COMMIT = 'enable.auto.commit';

    /**
     * The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if enable.auto.commit is set
     * to true.
     */
    public const string AUTO_COMMIT_INTERVAL_MS = 'auto.commit.interval.ms';

    /**
     * This option controls the retention time for topic offset storage, set to -1 to use broker retention time setting.
     *
     * It is the `retention_time` field of the OffsetCommit v2 request of Kafka 0.9, in milliseconds.
     */
    public const string OFFSET_RETENTION_MS = 'offset.retention.ms';

    /**
     * Deserializer that turns the raw bytes of a record key into an application-level value.
     *
     * Either an instance of {@see \Protocol\Kafka\Common\Serialization\Deserializer} or the name of a class that
     * implements it and can be constructed without arguments; null leaves the keys as raw byte strings.
     */
    public const string KEY_DESERIALIZER = 'key.deserializer';

    /**
     * Deserializer that turns the raw bytes of a record value into an application-level value, see
     * {@see self::KEY_DESERIALIZER}.
     */
    public const string VALUE_DESERIALIZER = 'value.deserializer';

    /**
     * Automatically check the CRC32 of the consumed records.
     *
     * This ensures no on-the-wire or on-disk corruption to the messages occurred; the check adds some overhead, so
     * a consumer that trusts its network may switch it off in cases seeking extreme performance. A message whose
     * checksum does not match is reported as a CorruptMessageException for its own partition.
     */
    public const string CHECK_CRCS = 'check.crcs';

    /**
     * Which records of a transactional topic this consumer is allowed to see (KIP-98, Kafka 0.11).
     *
     * The two values are the ones of the Java consumer, `read_uncommitted` and `read_committed`, and the wire
     * values behind them are {@see FetchRequest::READ_UNCOMMITTED} (0) and {@see FetchRequest::READ_COMMITTED} (1);
     * this client accepts either spelling. The default is `read_uncommitted`, which is what every broker below
     * 0.11 did and what the versions below 4 of the Fetch api do.
     *
     * With `read_committed` three things change at once:
     *
     * * the Fetch request states the level, and the broker answers **only up to the last stable offset** of a
     *   partition - the first offset of the oldest transaction that is still open - so the records of a running
     *   transaction are invisible, however far the high watermark has moved past them;
     * * the answer carries the transactions that were **aborted** in the range it covers, and the consumer drops
     *   the records of those producers itself: a 0.11.0.3 broker does *not* filter them out, it only names them;
     * * `endOffsets()` and the `Offsets` api answer the last stable offset instead of the high watermark, so a
     *   consumer that waits for the end of the log does not wait for records it will never be shown.
     *
     * A control batch - the COMMIT or ABORT marker the transaction coordinator appends - is never handed to an
     * application in either level.
     *
     * @see docs/protocol/4.3.md, section "Transactions"
     */
    public const string ISOLATION_LEVEL = 'isolation.level';

    /**
     * Rack of this consumer, the `rack_id` of a Fetch v11 request (KIP-392, Kafka 2.3).
     *
     * A rack-aware cluster puts the replicas of a partition into different racks (`broker.rack` of a broker), and
     * a consumer that reads across racks pays for the traffic twice - once inside the cluster, once out of it.
     * KIP-392 lets the consumer name its own rack in every fetch; the **leader** of the partition then picks a
     * replica for that rack with its `replica.selector.class` and answers the node id in the
     * `preferred_read_replica` of the partition entry, and the consumer reads from that broker until an answer
     * names another one, see {@see \Protocol\Kafka\Common\FetchedPartition::$preferredReadReplica}.
     *
     * The default is the empty string, "I am in no rack", which is also what every version below 11 says by
     * having no field at all. A broker without a `replica.selector.class` - the default, and the configuration of
     * the container of this line - answers `-1` to every fetch whatever the rack, i.e. "read from me".
     *
     * @see docs/protocol/4.3.md, section "Reading from a follower (v11, KIP-392)"
     */
    public const string CLIENT_RACK = 'client.rack';

    /**
     * `isolation.level` of a consumer that sees every record of the log, the default
     */
    public const string ISOLATION_LEVEL_READ_UNCOMMITTED = 'read_uncommitted';

    /**
     * `isolation.level` of a consumer that only sees the records of committed transactions
     */
    public const string ISOLATION_LEVEL_READ_COMMITTED = 'read_committed';

    public const string EXCLUDE_INTERNAL_TOPICS = 'exclude.internal.topics';

    /**
     * The maximum number of records a single poll() returns.
     *
     * {@see KafkaShareConsumer} sends it as the `max_records` and the `batch_size` of every ShareFetch, as the Java
     * share consumer does (`ShareSessionHandler` @ 4.3.1), with the default {@see self::DEFAULT_MAX_POLL_RECORDS};
     * how strictly the node keeps to it is {@see self::SHARE_ACQUIRE_MODE}. {@see KafkaConsumer} does not read it.
     */
    public const string MAX_POLL_RECORDS = 'max.poll.records';

    /**
     * Default of {@see self::MAX_POLL_RECORDS}, as in the Java consumer
     */
    public const int DEFAULT_MAX_POLL_RECORDS = 500;

    /**
     * How a share consumer acknowledges the records it is delivered: `implicit` (the default) or `explicit` (KIP-932)
     *
     * `share.acknowledgement.mode` of the Java consumer @ 4.3.1, read by {@see KafkaShareConsumer} alone.
     *
     * * **implicit** ({@see self::SHARE_ACKNOWLEDGEMENT_MODE_IMPLICIT}): every record a poll() returned is accepted
     *   by the next poll(), commitSync() or commitAsync(), and {@see KafkaShareConsumer::acknowledge()} must not be
     *   called; a close() releases what the last poll() returned instead of accepting it.
     * * **explicit** ({@see self::SHARE_ACKNOWLEDGEMENT_MODE_EXPLICIT}): the application acknowledges every record
     *   with {@see KafkaShareConsumer::acknowledge()} - accept, release, reject or renew - before its next poll(),
     *   which refuses to run otherwise.
     *
     * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
     */
    public const string SHARE_ACKNOWLEDGEMENT_MODE = 'share.acknowledgement.mode';

    /**
     * `share.acknowledgement.mode` of a share consumer whose poll() and commits accept what the last poll() returned
     */
    public const string SHARE_ACKNOWLEDGEMENT_MODE_IMPLICIT = 'implicit';

    /**
     * `share.acknowledgement.mode` of a share consumer that acknowledges every record itself
     */
    public const string SHARE_ACKNOWLEDGEMENT_MODE_EXPLICIT = 'explicit';

    /**
     * How the node reads the `max.poll.records` of a share consumer: `batch_optimized` (the default) or
     * `record_limit` (KIP-1206, ShareFetch **v2**, Kafka 4.2)
     *
     * `share.acquire.mode` of the Java consumer @ 4.3.1, the `share_acquire_mode` of every ShareFetch of
     * {@see KafkaShareConsumer}:
     *
     * * **batch_optimized** ({@see self::SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED}, the wire value 0): the node acquires
     *   whole record batches, so a poll() may return more than {@see self::MAX_POLL_RECORDS} records - the rest of a
     *   batch it started;
     * * **record_limit** ({@see self::SHARE_ACQUIRE_MODE_RECORD_LIMIT}, the wire value 1): the node acquires at most
     *   {@see self::MAX_POLL_RECORDS} records, and cuts a batch to do so.
     *
     * @see docs/protocol/4.3.md, section "The acquire mode and the renew acknowledgement (v2, KIP-1206 and KIP-1222)"
     */
    public const string SHARE_ACQUIRE_MODE = 'share.acquire.mode';

    /**
     * `share.acquire.mode` of a share consumer that lets the node acquire whole record batches
     */
    public const string SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED = 'batch_optimized';

    /**
     * `share.acquire.mode` of a share consumer that has the node acquire no more than `max.poll.records` records
     */
    public const string SHARE_ACQUIRE_MODE_RECORD_LIMIT = 'record_limit';

    /**
     * The options a share consumer refuses, as `ShareConsumerConfig` @ 4.3.1 lists them
     *
     * A share group has no committed offsets, no assignor of the client, no static membership and no session of the
     * client: where to start reading is the group config `share.auto.offset.reset`, the isolation level is the group
     * config `share.isolation.level`, and the session timeout and the heartbeat interval are the coordinator's.
     * `interceptor.classes` of the Java list has no counterpart in this package.
     *
     * @var list<string>
     */
    public const array SHARE_GROUP_UNSUPPORTED_CONFIGS = [
        self::AUTO_OFFSET_RESET,
        self::ENABLE_AUTO_COMMIT,
        self::GROUP_INSTANCE_ID,
        self::ISOLATION_LEVEL,
        self::PARTITION_ASSIGNMENT_STRATEGY,
        self::SESSION_TIMEOUT_MS,
        self::HEARTBEAT_INTERVAL_MS,
        self::GROUP_PROTOCOL,
        self::GROUP_REMOTE_ASSIGNOR,
    ];

    /**
     * Returns default configuration for consumer
     */
    public static function getDefaultConfiguration(): array
    {
        return self::$consumerConfiguration + parent::$generalConfiguration;
    }
}
