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

/**
 * Consumer config enumeration class
 *
 * Kafka 0.9.0.1 brings the broker-side group management, so the options that drive it exist here: session.timeout.ms,
 * heartbeat.interval.ms and partition.assignment.strategy, plus offset.retention.ms for the `retention_time` of the
 * OffsetCommit v2 request. Kafka 0.10.1 adds fetch.max.bytes, the request-level bound of a Fetch v3 answer. What
 * arrived later is absent: isolation.level (the transactional protocol of 0.11). The `offsets.storage` option of
 * the general config ({@see GeneralConfig::OFFSETS_STORAGE}) still selects where the committed offsets live
 * (OffsetCommit v0 vs v2).
 *
 * A consumer overrides one option of the general config: `request.timeout.ms` defaults to 40000 instead of 30000,
 * as it does in the Java consumer of 0.9.0.1, because it has to be larger than `session.timeout.ms` - the socket
 * would otherwise time out on a JoinGroup that the coordinator holds until the rebalance of the group is over.
 */
final class ConsumerConfig extends GeneralConfig
{
    /**
     * Default configuration for consumer
     */
    private static array $consumerConfiguration = [
        /* Used configs */
        ConsumerConfig::GROUP_ID                      => '',
        ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => 'range',
        // Larger than SESSION_TIMEOUT_MS below, as in the Java consumer of 0.9.0.1: the coordinator answers a
        // JoinGroup only once the whole rebalance is over, which can take a full session timeout
        ConsumerConfig::REQUEST_TIMEOUT_MS            => 40000,
        ConsumerConfig::SESSION_TIMEOUT_MS            => 30000,
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
        ConsumerConfig::KEY_DESERIALIZER              => null,
        ConsumerConfig::VALUE_DESERIALIZER            => null,
    ];

    /**
     * A unique string that identifies the consumer group this consumer belongs to.
     *
     * This property is required if the consumer uses either the group management functionality by using
     * subscribe(topic) or the Kafka-based offset management strategy.
     */
    public const string GROUP_ID = 'group.id';

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
     */
    public const string SESSION_TIMEOUT_MS = 'session.timeout.ms';

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

    public const string EXCLUDE_INTERNAL_TOPICS = 'exclude.internal.topics';
    public const string MAX_POLL_RECORDS        = 'max.poll.records';

    /**
     * Returns default configuration for consumer
     */
    public static function getDefaultConfiguration(): array
    {
        return self::$consumerConfiguration + parent::$generalConfiguration;
    }
}
