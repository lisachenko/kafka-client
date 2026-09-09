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
 * @date   29.07.2016
 */

namespace Protocol\Kafka\Producer;

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig as GeneralConfig;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Message;

/**
 * Producer config enumeration class
 *
 * Kafka 0.9.0.1 has neither idempotent nor transactional delivery - both arrived with 0.11 - so this branch carries
 * no `transactional.id` and no `enable.idempotence`.
 */
final class ProducerConfig extends GeneralConfig
{
    /**
     * Default configuration for producer (should be applied on top of default config)
     *
     * @var array<string, mixed>
     */
    protected static $producerConfiguration = [
        ProducerConfig::PARTITIONER_CLASS => DefaultPartitioner::class,
        ProducerConfig::ACKS              => 1,
        ProducerConfig::TIMEOUT_MS        => 2000,
        ProducerConfig::RETRIES           => 0,
        ProducerConfig::BATCH_SIZE        => 0,

        ProducerConfig::COMPRESSION_TYPE       => ProducerConfig::COMPRESSION_TYPE_NONE,
        ProducerConfig::LINGER_MS              => 0,
        ProducerConfig::MAX_REQUEST_SIZE       => 1048576,
        ProducerConfig::MESSAGE_FORMAT_VERSION => ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0,
    ];

    /**
     * The number of acknowledgments the producer requires the leader to have received before considering a request
     * complete. This controls the durability of records that are sent. The following settings are common:
     *
     * acks=0 If set to zero then the producer will not wait for any acknowledgment from the server at all. The record
     * will be immediately added to the socket buffer and considered sent. No guarantee can be made that the server has
     * received the record in this case, and the retries configuration will not take effect (as the client won't
     * generally know of any failures). The offset given back for each record will always be set to -1.
     *
     * acks=1 This will mean the leader will write the record to its local log but will respond without awaiting full
     * acknowledgement from all followers. In this case should the leader fail immediately after acknowledging the
     * record but before the followers have replicated it then the record will be lost.
     *
     * acks=all This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This
     * guarantees that the record will not be lost as long as at least one in-sync replica remains alive. This is the
     * strongest available guarantee. On the wire this is the value -1, {@see ProducerConfig::ACKS_ALL}.
     */
    public const string ACKS = 'acks';

    /**
     * The producer does not wait for an acknowledgement at all and the broker sends no response
     */
    public const int ACKS_NONE = 0;

    /**
     * The leader acknowledges the record as soon as it wrote it into its local log
     */
    public const int ACKS_LEADER = 1;

    /**
     * Every in-sync replica has to acknowledge the record
     */
    public const int ACKS_ALL = -1;

    /**
     * Partitioner class that implements the Partitioner interface.
     */
    public const string PARTITIONER_CLASS = 'partitioner.class';

    /**
     * Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially
     * transient error.
     *
     * Note that this retry is no different than if the client resent the record upon receiving the
     * error. Allowing retries without setting max.in.flight.requests.per.connection to 1 will potentially change the
     * ordering of records because if two batches are sent to a single partition, and the first fails and is retried
     * but the second succeeds, then the records in the second batch may appear first.
     *
     * This is the same option as {@see GeneralConfig::RETRIES}, and the default of the producer - no retry at all,
     * as with the Java producer - deliberately replaces the default of the general client configuration. It is the
     * whole retry budget of a batch: {@see Client::produce()} refreshes the cluster metadata and sends the
     * topic-partitions that failed with a retriable error again, this many times with `retry.backoff.ms` in between,
     * and {@see KafkaProducer::flush()} adds no second layer of retries on top of it.
     */
    public const string RETRIES = 'retries';

    /**
     * The producer will attempt to batch records together into fewer requests whenever multiple records are being sent
     * to the same partition. This helps performance on both the client and the server. This configuration controls the
     * default batch size in bytes.
     *
     * No attempt will be made to batch records larger than this size.
     *
     * Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent.
     *
     * A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable
     * batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a
     * buffer of the specified batch size in anticipation of additional records.
     */
    public const string BATCH_SIZE = 'batch.size';

    /**
     * The configuration controls the maximum amount of time the server will wait for acknowledgments from followers to
     * meet the acknowledgment requirements the producer has specified with the acks configuration. If the requested
     * number of acknowledgments are not met when the timeout elapses an error will be returned. This timeout is
     * measured on the server side and does not include the network latency of the request.
     */
    public const string TIMEOUT_MS = 'timeout.ms';

    /**
     * The compression type for all data generated by the producer: `none`, `gzip`, `snappy` or `lz4`.
     *
     * Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio:
     * more batching means better compression. A compressed batch is a single message of the produced message set
     * whose value is the whole batch, {@see CompressionCodec}.
     */
    public const string COMPRESSION_TYPE = 'compression.type';

    /**
     * The records of a batch are sent as they are, without compression
     */
    public const string COMPRESSION_TYPE_NONE = 'none';

    /**
     * The batch is compressed with gzip (RFC 1952)
     */
    public const string COMPRESSION_TYPE_GZIP = 'gzip';

    /**
     * The batch is compressed with the xerial framing of snappy that the brokers and the Java clients use
     */
    public const string COMPRESSION_TYPE_SNAPPY = 'snappy';

    /**
     * The batch is compressed with lz4, in the LZ4 frame format that the brokers and the Java clients use
     */
    public const string COMPRESSION_TYPE_LZ4 = 'lz4';

    /**
     * The message format this producer writes, named after the Kafka release that introduced it.
     *
     * It is the client-side counterpart of the `message.format.version` of a topic: a 0.10.2 broker stores what it
     * is configured to store and converts whatever the producer sent, so the option does not change what ends up in
     * the log - it only decides whether the broker has to convert the batch on append. Leave it at
     * {@see ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0} (message format v1, with timestamps) unless the topic is
     * configured with `message.format.version=0.9.0` or lower, where writing message format v0 straight away saves
     * the broker the conversion.
     *
     * @see docs/protocol/0.10.2.md, section "MessageSet and Message"
     */
    public const string MESSAGE_FORMAT_VERSION = 'message.format.version';

    /**
     * Message format v0: no timestamp, absolute inner offsets, the format of Kafka 0.8 and 0.9
     */
    public const string MESSAGE_FORMAT_VERSION_0_9_0 = '0.9.0';

    /**
     * Message format v1: an int64 timestamp, a timestamp type and relative inner offsets, since Kafka 0.10.0
     */
    public const string MESSAGE_FORMAT_VERSION_0_10_0 = '0.10.0';

    /**
     * The producer groups together any records that arrive in between request transmissions into a single batched
     * request.
     *
     * This setting gives the upper bound on the delay for batching: once the producer got `batch.size` worth of
     * records for a partition it will be sent immediately regardless of this setting, however if we have fewer than
     * this many bytes accumulated for this partition we will 'linger' for the specified time waiting for more records
     * to show up.
     *
     * This client has no background sender thread, so the delay is not a timer: a batch that has been lingering for
     * longer than `linger.ms` is sent by the next call to {@see KafkaProducer::send()}, and {@see
     * KafkaProducer::flush()} always sends everything that is buffered.
     */
    public const string LINGER_MS = 'linger.ms';

    /**
     * The maximum size of a request in bytes.
     *
     * This is also effectively a cap on the maximum record size. Note that the server has its own cap on record size
     * (`message.max.bytes`, one megabyte by default in 0.9.0.1) which may be different from this. A record that
     * exceeds this size is rejected by {@see KafkaProducer::send()} before it reaches the broker, and a buffer that
     * would grow past it is flushed first.
     */
    public const string MAX_REQUEST_SIZE = 'max.request.size';

    /**
     * Compression codec of every supported value of the `compression.type` option
     *
     * @var array<string, int>
     */
    private const array COMPRESSION_CODECS = [
        self::COMPRESSION_TYPE_NONE   => CompressionCodec::NONE,
        self::COMPRESSION_TYPE_GZIP   => CompressionCodec::GZIP,
        self::COMPRESSION_TYPE_SNAPPY => CompressionCodec::SNAPPY,
        self::COMPRESSION_TYPE_LZ4    => CompressionCodec::LZ4,
    ];

    /**
     * Magic byte of every supported value of the `message.format.version` option
     *
     * The releases below 0.10.0 all wrote message format v0, so their names are accepted as well.
     *
     * @var array<string, int>
     */
    private const array MESSAGE_FORMAT_MAGICS = [
        '0.8.0'                            => Message::MAGIC_V0,
        '0.8.1'                            => Message::MAGIC_V0,
        '0.8.2'                            => Message::MAGIC_V0,
        self::MESSAGE_FORMAT_VERSION_0_9_0 => Message::MAGIC_V0,
        '0.10.0'                           => Message::MAGIC_V1,
        '0.10.1'                           => Message::MAGIC_V1,
        '0.10.2'                           => Message::MAGIC_V1,
    ];

    /**
     * Returns default configuration for producer
     *
     * @return array<string, mixed>
     */
    public static function getDefaultConfiguration(): array
    {
        return self::$producerConfiguration + parent::$generalConfiguration;
    }

    /**
     * Resolves the `compression.type` option into the codec that the wire format announces.
     *
     * @param string|int $compressionType Name of the codec, or one of the {@see CompressionCodec} constants
     *
     * @throws InvalidConfigurationException for a codec that this client can not write
     */
    public static function compressionCodec(string|int $compressionType): int
    {
        if (is_int($compressionType)) {
            if (!CompressionCodec::isSupported($compressionType)) {
                throw new InvalidConfigurationException(
                    "Unsupported compression codec {$compressionType} configured for the producer"
                );
            }

            return $compressionType;
        }

        $normalizedType = strtolower(trim($compressionType));
        if (!isset(self::COMPRESSION_CODECS[$normalizedType])) {
            $supportedTypes = implode(', ', array_keys(self::COMPRESSION_CODECS));

            throw new InvalidConfigurationException(
                "Unsupported compression type \"{$compressionType}\", expected one of: {$supportedTypes}"
            );
        }

        return self::COMPRESSION_CODECS[$normalizedType];
    }

    /**
     * Resolves the `message.format.version` option into the magic byte that the messages of a batch carry.
     *
     * The value is a Kafka release, the way the broker spells the same option, or a magic byte; only the two formats
     * of this protocol line exist, so everything up to 0.9.0 is message format v0 and 0.10.x is message format v1.
     *
     * @param string|int $messageFormatVersion Name of a Kafka release, or one of the {@see Message} magic constants
     *
     * @throws InvalidConfigurationException for a message format that this client can not write
     */
    public static function messageFormatMagic(string|int $messageFormatVersion): int
    {
        if (is_int($messageFormatVersion)) {
            if ($messageFormatVersion !== Message::MAGIC_V0 && $messageFormatVersion !== Message::MAGIC_V1) {
                throw new InvalidConfigurationException(
                    "Unsupported message format magic {$messageFormatVersion} configured for the producer"
                );
            }

            return $messageFormatVersion;
        }

        // The broker accepts the full release name as well, e.g. `0.10.2-IV0` or `0.9.0.1`, and only the first
        // three components of it select the message format
        $normalizedVersion = strtolower(trim($messageFormatVersion));
        $normalizedVersion = explode('-', $normalizedVersion)[0];
        $normalizedVersion = implode('.', array_slice(explode('.', $normalizedVersion), 0, 3));

        if (!isset(self::MESSAGE_FORMAT_MAGICS[$normalizedVersion])) {
            $supportedVersions = implode(', ', array_keys(self::MESSAGE_FORMAT_MAGICS));

            throw new InvalidConfigurationException(
                "Unsupported message format version \"{$messageFormatVersion}\", expected one of: {$supportedVersions}"
            );
        }

        return self::MESSAGE_FORMAT_MAGICS[$normalizedVersion];
    }
}
