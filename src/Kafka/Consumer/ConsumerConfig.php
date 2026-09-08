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
 * Kafka 0.8.2.2 has no broker-side group management, so the options that only drive it (the session timeout, the
 * keep-alive interval of a group member and partition.assignment.strategy) do not exist on this branch.
 */
final class ConsumerConfig extends GeneralConfig
{
    /**
     * Default configuration for consumer
     */
    private static array $consumerConfiguration = [
        /* Used configs */
        ConsumerConfig::GROUP_ID                  => '',
        ConsumerConfig::FETCH_MIN_BYTES           => 1,
        ConsumerConfig::FETCH_MAX_WAIT_MS         => 500,
        ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 65536,
        ConsumerConfig::AUTO_OFFSET_RESET         => OffsetResetStrategy::LATEST,
        ConsumerConfig::ENABLE_AUTO_COMMIT        => true,
        ConsumerConfig::AUTO_COMMIT_INTERVAL_MS   => 0, // Commit always after each poll()
    ];

    /**
     * A unique string that identifies the consumer group this consumer belongs to.
     *
     * This property is required if the consumer uses the Kafka-based offset management strategy, because the
     * committed offsets are stored per consumer group.
     */
    public const string GROUP_ID = 'group.id';

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


    public const string KEY_DESERIALIZER        = 'key.deserializer';
    public const string VALUE_DESERIALIZER      = 'value.deserializer';
    public const string EXCLUDE_INTERNAL_TOPICS = 'exclude.internal.topics';
    public const string MAX_POLL_RECORDS        = 'max.poll.records';
    public const string CHECK_CRCS              = 'check.crcs';

    /**
     * Returns default configuration for consumer
     */
    public static function getDefaultConfiguration(): array
    {
        return self::$consumerConfiguration + parent::$generalConfiguration;
    }
}
