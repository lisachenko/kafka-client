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
use Protocol\Kafka\Common\Record\RecordBatch;

/**
 * Producer config enumeration class
 *
 * Kafka 0.11 added the delivery guarantee of KIP-98 to the producer, and with it the options that turn it on:
 * {@see ProducerConfig::ENABLE_IDEMPOTENCE} for the idempotent producer, {@see ProducerConfig::TRANSACTIONAL_ID}
 * for the transactional one - which implies the first - and the {@see ProducerConfig::TRANSACTION_TIMEOUT_MS} that
 * a transactional `InitProducerId` states.
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
        ProducerConfig::MESSAGE_FORMAT_VERSION => ProducerConfig::MESSAGE_FORMAT_VERSION_0_11_0,

        ProducerConfig::ENABLE_IDEMPOTENCE     => false,
        ProducerConfig::TRANSACTION_TIMEOUT_MS => 60000,
        ProducerConfig::TRANSACTIONAL_ID       => null,
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
     * It is the client-side counterpart of the `message.format.version` of a topic: a 0.11 broker stores what it
     * is configured to store and converts whatever the producer sent, so the option does not change what ends up in
     * the log - it only decides whether the broker has to convert the batch on append. Leave it at
     * {@see ProducerConfig::MESSAGE_FORMAT_VERSION_0_11_0} (message format v2, the record batch) unless the topic
     * is configured with an older `message.format.version`, where writing that format straight away saves the
     * broker the conversion.
     *
     * The format also decides the **version of the Produce request** the client sends, and with it what a record
     * may carry: only the message format v2 travels in a Produce v3 request, and only it has a place for record
     * headers, for the producer id and the sequence numbers of an idempotent producer and for a transactional id.
     * A batch of the formats v0 and v1 is sent as Produce v2 and its headers are dropped.
     *
     * @see docs/protocol/1.1.md, sections "MessageSet and Message" and "RecordBatch (message format v2)"
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
     * Message format v2: the record batch with headers, producer ids and transactions, since Kafka 0.11.0
     */
    public const string MESSAGE_FORMAT_VERSION_0_11_0 = '0.11.0';

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
     * (`message.max.bytes`, one megabyte by default in 0.10.2.2) which may be different from this. A record that
     * exceeds this size is rejected by {@see KafkaProducer::send()} before it reaches the broker, and a buffer that
     * would grow past it is flushed first.
     */
    public const string MAX_REQUEST_SIZE = 'max.request.size';

    /**
     * When set to `true`, the producer ensures that exactly one copy of each message is written in the stream.
     *
     * If `false`, producer retries due to broker failures may write duplicates of the retried message in the
     * stream. This is set to `false` by default, exactly as in the Java producer of 0.11.
     *
     * The guarantee is the one of KIP-98 and it is a guarantee **within one producer session**: the producer asks
     * a broker for a producer id with its first batch ({@see \Protocol\Kafka\Client::initProducerId()}), numbers
     * the batches of every topic-partition with gapless sequence numbers, and the broker recognises a batch it has
     * already appended - a retry after an acknowledgement that got lost - and answers it with the offset of the
     * original append instead of writing it twice. A producer that is restarted gets a new producer id and can not
     * deduplicate against what the old one wrote; that is what a `transactional.id` is for.
     *
     * Enabling it constrains two other options, and this client validates them exactly as the Java producer does:
     *
     * * `acks` must be {@see ProducerConfig::ACKS_ALL}. An explicit `acks` of 0 or 1 together with
     *   `enable.idempotence = true` is a configuration error; leaving `acks` alone makes it `all`.
     * * `retries` must not be 0 - a producer that never retries has nothing to deduplicate. An explicit 0 is a
     *   configuration error; leaving `retries` alone makes it {@see ProducerConfig::DEFAULT_IDEMPOTENT_RETRIES}.
     *
     * The Java producer also forces `max.in.flight.requests.per.connection` to 1, because more than one request in
     * flight can reorder the batches of a partition and every reordering is an out-of-order sequence for the
     * broker. This client is synchronous - {@see KafkaProducer::flush()} writes one produce request and reads its
     * answer before the next one - so it has no such option and satisfies the requirement by construction.
     *
     * @see docs/protocol/1.1.md, section "The idempotent producer"
     */
    public const string ENABLE_IDEMPOTENCE = 'enable.idempotence';

    /**
     * The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update
     * from the producer before proactively aborting the ongoing transaction.
     *
     * It travels in the `InitProducerId` request and is only meaningful for a producer that has a transactional
     * id: with a `null` one a 0.11.0.3 broker ignores the field entirely. A value above the broker's
     * `transaction.max.timeout.ms` (900000 by default) is refused with the error code 50
     * (`InvalidTransactionTimeout`).
     */
    public const string TRANSACTION_TIMEOUT_MS = 'transaction.timeout.ms';

    /**
     * The id that identifies this producer across its restarts, and the option that turns transactions on.
     *
     * A producer that carries one is a **transactional producer**: it may group the records of several partitions
     * - and the offsets of a consumer group - into a transaction that a `read_committed` consumer either sees
     * whole or does not see at all, with
     * {@see KafkaProducer::initTransactions()}, {@see KafkaProducer::beginTransaction()},
     * {@see KafkaProducer::sendOffsetsToTransaction()}, {@see KafkaProducer::commitTransaction()} and
     * {@see KafkaProducer::abortTransaction()}.
     *
     * The id is what makes the guarantee survive a restart: `InitProducerId` answers it with the producer id that
     * `__transaction_state` holds for it and with an epoch **one higher** than the previous incarnation used, which
     * fences that incarnation for good, and it aborts whatever transaction that incarnation had left open. Two
     * producers must therefore never run with the same transactional id at the same time - the second one silently
     * kills the first.
     *
     * A transactional id **implies `enable.idempotence`** ({@see ProducerConfig::resolveIdempotence()}), so it
     * carries the same constraints: `acks` has to be `all` and `retries` must not be 0. The **empty string** is not
     * a transactional id - a broker answers it with the error code 42 - and is refused here as a configuration
     * error.
     *
     * @see docs/protocol/1.1.md, section "Transactions"
     */
    public const string TRANSACTIONAL_ID = 'transactional.id';

    /**
     * The `retries` an idempotent producer gets when the configuration does not name a value.
     *
     * The Java producer overrides the default to `Integer.MAX_VALUE` here, because its background sender bounds a
     * batch by `request.timeout.ms` rather than by a number of attempts. This client has no sender thread: `retries`
     * is a loop inside {@see \Protocol\Kafka\Client::produce()} that {@see KafkaProducer::flush()} blocks on, so an
     * unbounded budget would be an unbounded flush. Three attempts on top of the first one is the deliberate
     * deviation, and a caller that wants more simply configures `retries`.
     */
    public const int DEFAULT_IDEMPOTENT_RETRIES = 3;

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
        '0.10.0'                            => Message::MAGIC_V1,
        '0.10.1'                            => Message::MAGIC_V1,
        '0.10.2'                            => Message::MAGIC_V1,
        self::MESSAGE_FORMAT_VERSION_0_11_0 => RecordBatch::MAGIC,
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
     * Applies what {@see ProducerConfig::ENABLE_IDEMPOTENCE} implies for `acks` and `retries`.
     *
     * The two options are not independent of the guarantee: a batch that only the leader acknowledged can be lost
     * with that leader, and a producer that never retries has no duplicate to deduplicate. The Java producer of
     * 0.11 therefore *overrides* both when the caller left them alone and *refuses* the configuration when the
     * caller set them to something the guarantee can not live with, and this is the same rule - which is why it
     * takes the options as the caller wrote them, before the defaults have been merged into them.
     *
     * A **`transactional.id` implies `enable.idempotence`**, as it does in the Java producer: a transaction is
     * built on the producer id and the sequence numbers of KIP-98, so there is no such thing as a transactional
     * producer that is not idempotent. An explicit `enable.idempotence = false` next to a transactional id is
     * therefore a configuration error, and so is the empty string as an id, which a broker answers with the error
     * code 42.
     *
     * @param array<string, mixed> $configuration Options of the caller, without the defaults
     *
     * @return array<string, mixed> The same options with the overrides of an idempotent producer applied
     *
     * @throws InvalidConfigurationException For an `acks` other than `all` or a `retries` of 0 next to
     *         `enable.idempotence = true`, and for a `transactional.id` that the guarantee can not live with
     */
    public static function resolveIdempotence(array $configuration): array
    {
        $transactionalId = $configuration[self::TRANSACTIONAL_ID] ?? null;
        if ($transactionalId !== null) {
            if (!is_string($transactionalId) || trim($transactionalId) === '') {
                throw new InvalidConfigurationException(
                    self::TRANSACTIONAL_ID . ' must be a non-empty string, "'
                    . (is_scalar($transactionalId) ? (string) $transactionalId : get_debug_type($transactionalId))
                    . '" given'
                );
            }
            if (array_key_exists(self::ENABLE_IDEMPOTENCE, $configuration)
                && !self::isIdempotenceEnabled($configuration[self::ENABLE_IDEMPOTENCE])
            ) {
                throw new InvalidConfigurationException(
                    'Cannot set ' . self::ENABLE_IDEMPOTENCE . ' to false while a ' . self::TRANSACTIONAL_ID
                    . ' is configured: a transaction is built on the producer id and the sequence numbers of the '
                    . 'idempotent producer'
                );
            }
            $configuration[self::ENABLE_IDEMPOTENCE] = true;
        }

        if (!self::isIdempotenceEnabled($configuration[self::ENABLE_IDEMPOTENCE] ?? false)) {
            return $configuration;
        }

        if (array_key_exists(self::ACKS, $configuration)) {
            $acks = $configuration[self::ACKS];
            if (self::parseAcks($acks) !== self::ACKS_ALL) {
                throw new InvalidConfigurationException(
                    'Must set ' . self::ACKS . ' to all in order to use the idempotent producer, "'
                    . (is_scalar($acks) ? (string) $acks : get_debug_type($acks)) . '" given'
                );
            }
        }
        // Also normalizes the string `all` into the -1 of the wire, which is what the request is built from
        $configuration[self::ACKS] = self::ACKS_ALL;

        if (array_key_exists(self::RETRIES, $configuration)) {
            if ((int) $configuration[self::RETRIES] === 0) {
                throw new InvalidConfigurationException(
                    'Must set ' . self::RETRIES . ' to non-zero when using the idempotent producer'
                );
            }
        } else {
            $configuration[self::RETRIES] = self::DEFAULT_IDEMPOTENT_RETRIES;
        }

        return $configuration;
    }

    /**
     * Tells whether a value of the `enable.idempotence` option turns the guarantee on.
     *
     * The option is a boolean in the Java client, and a configuration that was read out of a `.properties` file
     * carries it as the string `"true"`, so both spellings are accepted here.
     */
    public static function isIdempotenceEnabled(mixed $value): bool
    {
        if (is_string($value)) {
            return strtolower(trim($value)) === 'true';
        }

        return (bool) $value;
    }

    /**
     * Resolves the `acks` option into the number of acknowledgements that goes on the wire
     *
     * The Java producer spells "every in-sync replica" as the string `all`, which is the value -1 of the wire.
     */
    public static function parseAcks(mixed $acks): int
    {
        if (is_string($acks) && strtolower(trim($acks)) === 'all') {
            return self::ACKS_ALL;
        }

        return (int) $acks;
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
     * The value is a Kafka release, the way the broker spells the same option, or a magic byte: everything up to
     * 0.9.0 is message format v0, 0.10.x is message format v1 and 0.11.0 is the record batch of message format v2.
     *
     * @param string|int $messageFormatVersion Name of a Kafka release, or one of the {@see Message} magic constants
     *
     * @throws InvalidConfigurationException for a message format that this client can not write
     */
    public static function messageFormatMagic(string|int $messageFormatVersion): int
    {
        if (is_int($messageFormatVersion)) {
            if (!in_array($messageFormatVersion, [Message::MAGIC_V0, Message::MAGIC_V1, RecordBatch::MAGIC], true)) {
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
