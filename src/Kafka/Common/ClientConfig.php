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

namespace Protocol\Kafka\Common;

/**
 * General config, suitable for both producer and consumer
 *
 * Kafka 0.8.2.2 knows no transport security at all - encryption and authentication only arrived with 0.9 - so this
 * branch carries no ssl.*, security.protocol or authentication-mechanism options.
 */
class ClientConfig
{
    protected static $generalConfiguration = [
        ClientConfig::BOOTSTRAP_SERVERS            => [],
        ClientConfig::CLIENT_ID                    => 'PHP/Kafka',
        ClientConfig::STREAM_PERSISTENT_CONNECTION => false,
        ClientConfig::STREAM_ASYNC_CONNECT         => false,
        ClientConfig::METADATA_MAX_AGE_MS          => 300000,
        ClientConfig::RECEIVE_BUFFER_BYTES         => 32768,
        ClientConfig::SEND_BUFFER_BYTES            => 131072,

        ClientConfig::CONNECTIONS_MAX_IDLE_MS   => 540000,
        ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
        ClientConfig::METADATA_FETCH_TIMEOUT_MS => 60000,
        ClientConfig::RECONNECT_BACKOFF_MS      => 50,
        ClientConfig::RETRY_BACKOFF_MS          => 100,
        ClientConfig::OFFSETS_STORAGE           => 'kafka',
    ];

    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     */
    public const BOOTSTRAP_SERVERS = 'bootstrap.servers';

    /**
     * An id string to pass to the server when making requests.
     *
     * The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical
     * application name to be included in server-side request logging.
     */
    public const CLIENT_ID = 'client.id';

    /**
     * The configuration controls the maximum amount of time the client will wait for the response of a request.
     *
     * If the response is not received before the timeout elapses the client will resend the request if necessary or
     * fail the request if retries are exhausted.
     */
    public const REQUEST_TIMEOUT_MS = 'request.timeout.ms';

    /**
     * Where the consumer offsets are stored: in Kafka itself or in ZooKeeper.
     *
     * Kafka 0.8.2 introduced Kafka-based offset storage (OffsetCommit/OffsetFetch v1) while keeping the ZooKeeper
     * storage of 0.8.1 available through v0 of the same requests. This option selects the storage that the client
     * uses; only the `kafka` value is meaningful for a broker-only client.
     */
    public const OFFSETS_STORAGE = 'offsets.storage';

    /**
     * Offsets are committed to and fetched from the offset coordinator (OffsetCommit/OffsetFetch v1)
     */
    public const OFFSETS_STORAGE_KAFKA = 'kafka';

    /**
     * Offsets are committed to and fetched from ZooKeeper (OffsetCommit/OffsetFetch v0)
     */
    public const OFFSETS_STORAGE_ZOOKEEPER = 'zookeeper';

    /**
     * Should client use persistent connection to the cluster or not
     *
     * (PHP Only option)
     */
    public const STREAM_PERSISTENT_CONNECTION = 'stream.persistent.connection';

    /**
     * Should client use asynchronous connection to the broker
     *
     * (PHP Only option)
     */
    public const STREAM_ASYNC_CONNECT = 'stream.async.connect';

    /**
     * File name that stores the metadata, this file will be effectively cached by the Opcode cache in production
     *
     * (PHP Only option)
     */
    public const METADATA_CACHE_FILE = 'metadata.cache.file';

    /**
     * The first time data is sent to the broker we must fetch metadata about that topic to know which servers host the
     * topic's partitions. This fetch to succeed before throwing an exception back to the client.
     */
    public const METADATA_FETCH_TIMEOUT_MS = 'metadata.fetch.timeout.ms';

    /**
     * The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any
     * partition leadership changes to proactively discover any new brokers or partitions.
     *
     * Applied only if the metadata.cache.file is configured
     */
    public const METADATA_MAX_AGE_MS = 'metadata.max.age.ms';

    /**
     * The size of the TCP send buffer (SO_SNDBUF) to use when sending data.
     */
    public const SEND_BUFFER_BYTES = 'send.buffer.bytes';

    /**
     * The size of the TCP receive buffer (SO_RCVBUF) to use when reading data.
     */
    public const RECEIVE_BUFFER_BYTES = 'receive.buffer.bytes';

    public const CONNECTIONS_MAX_IDLE_MS = 'connections.max.idle.ms';
    public const RECONNECT_BACKOFF_MS    = 'reconnect.backoff.ms';
    public const RETRY_BACKOFF_MS        = 'retry.backoff.ms';

    /**
     * Returns default configuration
     */
    public static function getDefaultConfiguration(): array
    {
        return self::$generalConfiguration;
    }
}
