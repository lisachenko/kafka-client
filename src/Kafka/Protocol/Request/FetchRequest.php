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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\FetchRequestTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV0;

/**
 * Fetch API (key 1), version 5
 *
 * The fetch API is used to fetch a chunk of one or more logs for some topic-partitions. Logically one specifies the
 * topics, partitions, and starting offset at which to begin the fetch and gets back a chunk of messages. In general,
 * the return messages will have offsets larger than or equal to the starting offset. However, with compressed
 * messages, it's possible for the returned messages to have offsets smaller than the starting offset. The number of
 * such messages is typically small and the caller is responsible for filtering out those messages.
 *
 * Fetch requests follow a long poll model so they can be made to block for a period of time if sufficient data is not
 * immediately available.
 *
 * As an optimization the server is allowed to return a partial message at the end of the message set. Clients should
 * handle this case.
 *
 * <pre>
 *   FetchRequest (Version: 5) => ReplicaId MaxWaitTime MinBytes MaxBytes IsolationLevel
 *                                [TopicName [Partition FetchOffset LogStartOffset MaxBytes]]
 *     ReplicaId      => int32
 *     MaxWaitTime    => int32
 *     MinBytes       => int32
 *     MaxBytes       => int32
 *     IsolationLevel => int8
 *     FetchOffset    => int64
 *     LogStartOffset => int64
 * </pre>
 *
 * The five versions of this api that a 0.11.0.3 broker serves next to this one differ as follows:
 *
 * * **v1** (Kafka 0.9) left the request untouched and only prefixed the answer with `ThrottleTimeMs`, see
 *   {@see FetchResponse};
 * * **v2** (Kafka 0.10.0) is byte-identical to v1 in both directions - `FETCH_REQUEST_V2` is `FETCH_REQUEST_V1` in
 *   `Protocol.java` @ 0.11.0.3 - and means one thing only: *the client understands message format v1*. A broker
 *   answers a request below version 2 by converting every stored record down to message format v0
 *   (`KafkaApis.handleFetchRequest`), which strips the timestamps and turns the relative inner offsets of a
 *   compressed set back into absolute ones;
 * * **v3** (Kafka 0.10.1, KIP-74) adds the request-level `MaxBytes` after `MinBytes`, which bounds the **whole**
 *   answer instead of a single partition, see {@see self::$maxBytes};
 * * **v4** (Kafka 0.11.0, KIP-98) adds the `IsolationLevel` after it and is the first version that is answered
 *   with the log as it lies: a record batch of the message format v2, with the headers, the producer ids and the
 *   transaction flags that no lower version has a place for. Its answer carries the `LastStableOffset` and the
 *   `AbortedTransactions` of every partition;
 * * **v5** (KIP-107) adds the `LogStartOffset` of a partition entry, which only a follower fills in, and the
 *   `LogStartOffset` of every partition of the answer.
 *
 * {@see FetchRequestV4}, {@see FetchRequestV3}, {@see FetchRequestV2}, {@see FetchRequestV1} and
 * {@see FetchRequestV0} keep the lower versions available.
 *
 * @see docs/protocol/0.11.0.md, section "Fetch API (key 1, v0 to v5)"
 */
class FetchRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::FETCH;

    /**
     * @inheritdoc
     */
    public const int VERSION = 5;

    /**
     * Default bound of a whole answer, the 50 MiB of the `fetch.max.bytes` option of the Java consumer
     *
     * @see \Protocol\Kafka\Consumer\ConsumerConfig::FETCH_MAX_BYTES
     */
    public const int DEFAULT_MAX_BYTES = 52428800;

    /**
     * `isolation_level` of version 4 and above (Kafka 0.11, KIP-98): every record of the log is visible, transactional
     * ones included, up to the high water mark. What the Offsets api v2 and `ConsumerConfig::ISOLATION_LEVEL` send
     * by default.
     */
    public const int READ_UNCOMMITTED = 0;

    /**
     * `isolation_level` of version 4 and above: only non-transactional and *committed* transactional records are
     * visible, and the fetch stops at the last stable offset instead of the high water mark.
     */
    public const int READ_COMMITTED = 1;

    /**
     * Topics to fetch from, indexed by the topic name
     *
     * @var array<string, FetchRequestTopic>
     */
    protected readonly array $topicPartitions;

    /**
     * @param array<string, array<int, int>> $topicPartitions   Fetch offset of every partition, as topic =>
     *                                                          partition => offset. The **order** of this array is
     *                                                          the order the broker fills the answer in, see
     *                                                          {@see self::$maxBytes}.
     * @param int                            $maxWaitTime       The maximum amount of time in milliseconds to block
     *                                                          waiting if insufficient data is available at the time
     *                                                          the request is issued.
     * @param int                            $minBytes          The minimum number of bytes of messages that must be
     *                                                          available to give a response. With 0 the server
     *                                                          always responds immediately, with 1 as soon as at
     *                                                          least one partition has at least one byte of data, or
     *                                                          when $maxWaitTime is over.
     * @param int                            $partitionMaxBytes The maximum number of bytes to include in the message
     *                                                          set of one partition, the `MaxBytes` of every
     *                                                          partition entry (`max.partition.fetch.bytes`).
     * @param int                            $replicaId         The node id of the replica that initiates this
     *                                                          request. Ordinary consumers always send -1 as they
     *                                                          have no node id; -2 is accepted from a non-broker
     *                                                          that wants to fetch as if it were a replica, for
     *                                                          debugging purposes.
     * @param int                            $maxBytes          The maximum number of bytes of the whole answer
     *                                                          (`fetch.max.bytes`), the field that version 3 added.
     */
    public function __construct(
        array $topicPartitions,
        protected readonly int $maxWaitTime,
        protected readonly int $minBytes,
        int $partitionMaxBytes,
        protected readonly int $replicaId = -1,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Maximum number of bytes the broker may put into the whole answer, since version 3 (KIP-74).
         *
         * The broker fills the partitions **in the order of the request** and stops once this budget is used up
         * (`ReplicaManager.readFromLocalLog` @ 0.10.2.2 subtracts the size of every partition it read from
         * `limitBytes`), so the partitions at the end of a request come back empty while the ones in front of them
         * carry data. A client that fetches more than one partition therefore has to rotate their order between
         * requests, otherwise the last ones would never be served.
         *
         * It is **not** a hard limit: as long as nothing has been read yet, the first non-empty partition ignores
         * both this bound and its own `MaxBytes` and returns at least one complete message, so a consumer always
         * makes progress. The other side of that coin is that an answer may exceed this value.
         */
        protected readonly int $maxBytes = self::DEFAULT_MAX_BYTES,
        /**
         * Visibility of transactional records, since version 4 (KIP-98).
         *
         * {@see self::READ_UNCOMMITTED} shows every record of the log up to the high water mark, transactional
         * ones included and whether or not their transaction was ever committed; {@see self::READ_COMMITTED} stops
         * the answer at the `LastStableOffset` of the partition and makes the broker report the transactions it
         * aborted in that range, so that the consumer can drop their records. A version below 4 does not carry the
         * field at all and always reads uncommitted.
         */
        protected readonly int $isolationLevel = self::READ_UNCOMMITTED
    ) {
        $topicClass            = static::topicClass();
        $partitionClass        = $topicClass::partitionClass();
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitionOffsets) {
            $partitions = [];
            foreach ($partitionOffsets as $partition => $fetchOffset) {
                $partitions[$partition] = new $partitionClass($partition, $fetchOffset, $partitionMaxBytes);
            }
            $packedTopicPartitions[$topic] = new $topicClass($topic, $partitions);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds a request from a list of topic partitions with the offset to start fetching each of them at
     *
     * The order of the pairs is kept, because it is the order in which the broker fills the answer of a version 3
     * request until its `MaxBytes` are used up.
     *
     * @param iterable<array{TopicPartition, int}> $partitionOffsets Pairs of a topic partition and its fetch offset
     */
    public static function fromTopicPartitions(
        iterable $partitionOffsets,
        int $maxWaitTime,
        int $minBytes,
        int $partitionMaxBytes,
        int $replicaId = -1,
        string $clientId = '',
        int $correlationId = 0,
        int $maxBytes = self::DEFAULT_MAX_BYTES,
        int $isolationLevel = self::READ_UNCOMMITTED
    ): static {
        $topicPartitions = [];
        foreach ($partitionOffsets as [$topicPartition, $fetchOffset]) {
            $topicPartitions[$topicPartition->topic][$topicPartition->partition] = $fetchOffset;
        }

        return new static(
            $topicPartitions,
            $maxWaitTime,
            $minBytes,
            $partitionMaxBytes,
            $replicaId,
            $clientId,
            $correlationId,
            $maxBytes,
            $isolationLevel
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'replicaId'   => BinarySchema::TYPE_INT32,
            'maxWaitTime' => BinarySchema::TYPE_INT32,
            'minBytes'    => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 3) {
            $body['maxBytes'] = BinarySchema::TYPE_INT32;
        }
        if (static::VERSION >= 4) {
            $body['isolationLevel'] = BinarySchema::TYPE_INT8;
        }
        $body['topicPartitions'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the isolation level this request asks for, {@see self::READ_UNCOMMITTED} below version 4
     */
    public function getIsolationLevel(): int
    {
        return static::VERSION >= 4 ? $this->isolationLevel : self::READ_UNCOMMITTED;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this request is sent as
     *
     * @return class-string<FetchRequestTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 5 ? FetchRequestTopic::class : FetchRequestTopicV0::class;
    }
}
