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

namespace Protocol\Kafka\Tests\Fixture;

/**
 * Builds the response frames of the Kafka 0.9.0.1 APIs, byte for byte as the specification describes them.
 *
 * <pre>
 *   Response => Size CorrelationId ResponseMessage
 *     Size          => int32
 *     CorrelationId => int32
 * </pre>
 *
 * The correlation id given here is only a placeholder: {@see BrokerConnection} replaces it with the one of the
 * request it answers, the same way a broker echoes it back.
 *
 * @see docs/protocol/4.3.md
 */
final class ResponseFrame
{
    /**
     * Wraps a response body into the common `Size CorrelationId` envelope
     */
    public static function of(int $correlationId, string $body): string
    {
        $payload = pack('N', $correlationId) . $body;

        return pack('N', strlen($payload)) . $payload;
    }

    /**
     * Wraps a body in a **flexible** answer (KIP-482, Kafka 2.4): the response header v1 carries a tag buffer
     * behind the correlation id, and the body of such a frame ends in one of its own
     *
     * @param string $body Body of the answer, its own tagged-field section NOT included
     */
    public static function flexible(int $correlationId, string $body): string
    {
        return self::of($correlationId, self::tagBuffer() . $body . self::tagBuffer());
    }

    /**
     * Encodes the empty tagged-field section that closes every structure of a flexible version
     */
    public static function tagBuffer(): string
    {
        return "\x00";
    }

    /**
     * Encodes an unsigned varint, the length type of every compact field (`ByteUtils.writeUnsignedVarint`)
     */
    public static function unsignedVarint(int $value): string
    {
        $bytes = '';
        while (($value & ~0x7F) !== 0) {
            $bytes .= chr(($value & 0x7F) | 0x80);
            $value >>= 7;
        }

        return $bytes . chr($value);
    }

    /**
     * Encodes a compact string: the unsigned varint `length + 1`, then the bytes; `0` is null
     */
    public static function compactString(?string $value): string
    {
        return $value === null
            ? self::unsignedVarint(0)
            : self::unsignedVarint(strlen($value) + 1) . $value;
    }

    /**
     * Encodes a compact byte array, which is the same shape as a compact string
     */
    public static function compactBytes(?string $value): string
    {
        return self::compactString($value);
    }

    /**
     * Encodes the element count of a compact array: the unsigned varint `count + 1`; `0` is null
     */
    public static function compactCount(int $count): string
    {
        return self::unsignedVarint($count + 1);
    }

    /**
     * Cluster id that {@see self::metadata()} answers with, 22 characters like the one a 0.10.1 broker generates
     */
    public const string CLUSTER_ID = 'kafka-client-test-clst';

    /**
     * Value of an `authorized_operations` bitfield the request did not ask for: `Integer.MIN_VALUE` (KIP-430)
     */
    public const int NOT_REQUESTED = -2147483648;

    /**
     * Builds a Metadata response (api key 3, v5 - the version this client sends)
     *
     * <pre>
     *   MetadataResponse => ThrottleTimeMs [Broker] ClusterId ControllerId [TopicMetadata]
     *     ThrottleTimeMs    => int32                          # since version 3 (KIP-124)
     *     Broker            => NodeId int32 Host string Port int32 Rack nullable string
     *     ClusterId         => nullable string
     *     ControllerId      => int32
     *     TopicMetadata     => TopicErrorCode int16 TopicName string IsInternal boolean [PartitionMetadata]
     *     PartitionMetadata => PartitionErrorCode int16 PartitionId int32 Leader int32 LeaderEpoch int32
     *                          Replicas [int32] Isr [int32] OfflineReplicas [int32]
     *                          LeaderEpoch     => int32        # since version 7 (KIP-320)
     *                          OfflineReplicas => [int32]      # since version 5 (KIP-112/113)
     * </pre>
     *
     * Version 4 answers the very same frame as version 3 - what it added, `allow_auto_topic_creation`, is a field
     * of the request - and version 5 (Kafka 1.0) appended `OfflineReplicas` to every partition entry, which is
     * always empty on the one-broker container of `docker-compose.yml`. The throttle time is always 0, like every
     * answer of that container, which sets no quota.
     *
     * The first broker of the list is the controller unless `$controllerId` says otherwise, and no broker declares
     * a rack - the answer of the container of `docker-compose.yml`, which runs a single broker without
     * `broker.rack`. A topic counts as internal when its name is in `$internalTopics`, i.e. `__consumer_offsets`
     * and `__transaction_state` on a 1.1.1 cluster.
     *
     * @param list<array{int, string, int}>  $brokers             nodeId, host, port
     * @param array<string, array<int, int>> $topics              topic => partition => leader node id
     * @param array<string, int>             $topicErrorCodes     Error code of a topic, if any
     * @param array<string, array<int, int>> $partitionErrorCodes Error code of a partition
     * @param list<string>                   $internalTopics      Topics to flag with `is_internal`
     * @param int|null                       $controllerId        Controller of the cluster, -1 while it elects one
     * @param array<string, array<int, list<int>>> $offlineReplicas Offline replicas of a partition, empty by
     *        default as on a one-broker cluster
     * @param array<string, array<int, int>> $leaderEpochs Leader epoch of a partition, the field version 7
     *        (Kafka 2.1, KIP-320) added; 0 by default, which is the epoch of a partition that has been led by the
     *        same broker since it was created
     */
    public static function metadata(
        int $correlationId,
        array $brokers,
        array $topics = [],
        array $topicErrorCodes = [],
        array $partitionErrorCodes = [],
        array $internalTopics = [],
        ?int $controllerId = null,
        array $offlineReplicas = [],
        array $leaderEpochs = [],
        array $topicAuthorizedOperations = [],
        array $topicIds = []
    ): string {
        // Version 9 (Kafka 2.4) is the first FLEXIBLE version of this api (KIP-482): every string and every
        // array announces its length as an unsigned varint of `length + 1`, and every structure - the body, a
        // broker, a topic, a partition - ends in a tagged-field section. Version 10 (Kafka 2.8, KIP-516) put the
        // `topic_id` of every topic between its name and its `is_internal` flag, and version 11 (KIP-700) took
        // the `cluster_authorized_operations` off the end of the frame again.
        $body = pack('N', 0) . self::compactArrayLength(count($brokers));
        foreach ($brokers as [$nodeId, $host, $port]) {
            // The rack of the broker, null for a cluster that is not rack aware
            $body .= pack('N', $nodeId) . self::compactString($host) . pack('N', $port)
                . self::compactString(null) . self::tagBuffer();
        }

        $body .= self::compactString(self::CLUSTER_ID);
        $body .= pack('N', $controllerId ?? $brokers[0][0] ?? -1);

        $body .= self::compactArrayLength(count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= pack('n', $topicErrorCodes[$topic] ?? 0) . self::compactString((string) $topic);
            // The topic id of KIP-516. Every topic of a broker from Kafka 2.8 on has one, so a caller that
            // names none gets a stable id derived from the topic name - the fetch path of Fetch v13 (Kafka 3.1)
            // can not name a topic without it. A caller that wants the zero id passes it explicitly.
            $body .= str_pad($topicIds[$topic] ?? self::topicIdOf((string) $topic), 16 /* Uuid::SIZE */, "\x00", STR_PAD_LEFT);
            $body .= pack('C', in_array((string) $topic, $internalTopics, true) ? 1 : 0);
            $body .= self::compactArrayLength(count($partitions));
            foreach ($partitions as $partitionId => $leader) {
                $replicas = $leader < 0 ? [] : [$leader];
                $body .= pack('n', $partitionErrorCodes[$topic][$partitionId] ?? 0)
                    . pack('N', $partitionId)
                    . pack('N', $leader)
                    // The leader epoch of version 7 (Kafka 2.1, KIP-320), behind the leader id
                    . pack('N', $leaderEpochs[$topic][$partitionId] ?? 0)
                    . self::compactInt32Array($replicas)
                    . self::compactInt32Array($replicas)
                    . self::compactInt32Array($offlineReplicas[$topic][$partitionId] ?? [])
                    . self::tagBuffer();
            }

            // The `topic_authorized_operations` bitfield of version 8 (Kafka 2.3, KIP-430), behind the
            // partitions; Integer.MIN_VALUE is what a broker writes when the request did not ask for it
            $body .= pack('N', $topicAuthorizedOperations[$topic] ?? self::NOT_REQUESTED) . self::tagBuffer();
        }

        // The `cluster_authorized_operations` of version 8 lived at the very end of the frame and is gone from
        // version 11 on (KIP-700), so the body simply ends in its tagged-field section
        $body .= self::tagBuffer();

        // The response header v1 of a flexible api: the correlation id and a tag buffer of its own
        return self::of($correlationId, self::tagBuffer() . $body);
    }

    /**
     * Encodes the length prefix of a compact array: the unsigned varint `count + 1`
     */
    public static function compactArrayLength(int $count): string
    {
        return self::unsignedVarint($count + 1);
    }

    /**
     * Encodes an int32 array of a flexible version: a compact length and the values
     *
     * @param list<int> $values
     */
    public static function compactInt32Array(array $values): string
    {
        $bytes = self::compactArrayLength(count($values));
        foreach ($values as $value) {
            $bytes .= pack('N', $value);
        }

        return $bytes;
    }

    /**
     * Builds a Produce response (api key 0, v5 - the version this client sends for the message format v2)
     *
     * The frames of the versions 2, 3 and 4 are one and the same (`PRODUCE_RESPONSE_V4` is `PRODUCE_RESPONSE_V3`
     * is `PRODUCE_RESPONSE_V2` @ 1.1.1); version 5 (Kafka 1.0) appended `LogStartOffset` to every partition entry.
     *
     * <pre>
     *   ProduceResponse => [TopicName [Partition ErrorCode Offset LogAppendTime LogStartOffset]] ThrottleTime
     * </pre>
     *
     * @param array<string, array<int, array{int, int}>> $topics        topic => partition => [errorCode, baseOffset]
     * @param int                                        $throttleTime  Milliseconds the broker delayed the request
     * @param int                                        $logAppendTime Time the broker stamped the batch with, -1
     *        for a topic that keeps the `CreateTime` of the producer
     * @param array<string, array<int, int>>             $logStartOffsets Log start offset of a partition, 0 by
     *        default as on a log nothing was deleted from
     */
    public static function produce(
        int $correlationId,
        array $topics,
        int $throttleTime = 0,
        int $logAppendTime = -1,
        array $logStartOffsets = [],
        array $recordErrors = []
    ): string {
        // The throttle time of v1 closes the response, the opposite end from where the Fetch API puts it, and
        // version 9 (Kafka 2.8, KIP-482) writes the whole frame with the compact types and the tagged-field
        // sections of a flexible version, behind a response header v1
        $body = self::produceTopics($topics, $logAppendTime, $logStartOffsets, $recordErrors, true)
            . pack('N', $throttleTime) . self::tagBuffer();

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a Produce response of the versions 2, 3 and 4, i.e. the same answer without `LogStartOffset`
     *
     * @param array<string, array<int, array{int, int}>> $topics topic => partition => [errorCode, baseOffset]
     */
    public static function produceV2(
        int $correlationId,
        array $topics,
        int $throttleTime = 0,
        int $logAppendTime = -1
    ): string {
        $body = self::produceTopics($topics, $logAppendTime, null) . pack('N', $throttleTime);

        return self::of($correlationId, $body);
    }

    /**
     * Builds a Produce response of version 0, i.e. the same answer without `LogAppendTime` and `ThrottleTime`
     *
     * @param array<string, array<int, array{int, int}>> $topics topic => partition => [errorCode, baseOffset]
     */
    public static function produceV0(int $correlationId, array $topics): string
    {
        return self::of($correlationId, self::produceTopics($topics, null, null));
    }

    /**
     * Builds the topics array of a Produce response
     *
     * @param array<string, array<int, array{int, int}>> $topics        topic => partition => [errorCode, baseOffset]
     * @param int|null                                   $logAppendTime Append time of every partition entry, or
     *        null for the versions 0 and 1, which do not carry that field at all
     * @param array<string, array<int, int>>|null         $logStartOffsets Log start offset of every partition
     *        entry, or null for the versions below 5, which do not carry that field at all
     */
    private static function produceTopics(
        array $topics,
        ?int $logAppendTime,
        ?array $logStartOffsets,
        ?array $recordErrors = null,
        bool $flexible = false
    ): string {
        $body = $flexible ? self::compactCount(count($topics)) : pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= $flexible
                ? self::compactString((string) $topic) . self::compactCount(count($partitions))
                : self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $baseOffset]) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode) . pack('J', $baseOffset);
                if ($logAppendTime !== null) {
                    $body .= pack('J', $logAppendTime);
                }
                if ($logStartOffsets !== null) {
                    $body .= pack('J', $logStartOffsets[$topic][$partitionId] ?? 0);
                }
                if ($logStartOffsets === null || $recordErrors === null) {
                    continue;
                }

                // The record errors and the error message of version 8 (Kafka 2.4, KIP-467), behind the log
                // start offset: the records of the sent batch that the broker refused, by their position in it
                [$errors, $message] = $recordErrors[$topic][$partitionId] ?? [[], null];
                $body .= $flexible ? self::compactCount(count($errors)) : pack('N', count($errors));
                foreach ($errors as $batchIndex => $batchMessage) {
                    $body .= pack('N', $batchIndex) . ($flexible
                        ? self::compactString($batchMessage) . self::tagBuffer()
                        : self::nullableString($batchMessage));
                }
                $body .= $flexible
                    ? self::compactString($message) . self::tagBuffer()
                    : self::nullableString($message);
            }
            if ($flexible) {
                $body .= self::tagBuffer();
            }
        }

        return $body;
    }

    /**
     * Builds an Offsets (ListOffset) response (api key 2, v2 - the version this client sends)
     *
     * <pre>
     *   ListOffsets Response (Version: 2) => throttle_time_ms [responses]
     *     partition_responses => partition error_code timestamp offset
     * </pre>
     *
     * The partition entries are the ones of version 1; version 2 (KIP-124) only put the throttle time in front of
     * the topics array, and it is always 0 here, as it is on the container.
     *
     * @param array<string, array<int, array{int, int, int}>> $topics topic => partition =>
     *        [errorCode, timestamp, offset]
     */
    public static function offsets(int $correlationId, array $topics, array $leaderEpochs = []): string
    {
        // Version 6 (Kafka 2.8, KIP-482) is the flexible version of the api: the response header v1, a compact
        // topic name, compact arrays and a tagged-field section behind every structure
        $body = pack('N', 0) . self::compactCount(count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::compactString((string) $topic) . self::compactCount(count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $timestamp, $offset]) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode) . pack('J', $timestamp) . pack('J', $offset)
                    // The leader epoch of version 4 (Kafka 2.1, KIP-320), behind the offset
                    . pack('N', $leaderEpochs[$topic][$partitionId] ?? 0xFFFFFFFF)
                    . self::tagBuffer();
            }
            $body .= self::tagBuffer();
        }

        return self::flexible($correlationId, $body . self::tagBuffer());
    }

    /**
     * Builds an Offsets (ListOffset) response of version 0, which answers a list of segment offsets per partition
     *
     * <pre>
     *   OffsetResponse => [TopicName [PartitionOffsets]]
     *     PartitionOffsets => Partition ErrorCode [Offset]
     * </pre>
     *
     * @param array<string, array<int, array{int, list<int>}>> $topics topic => partition => [errorCode, offsets]
     */
    public static function offsetsV0(int $correlationId, array $topics): string
    {
        $body = pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $offsets]) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode) . pack('N', count($offsets));
                foreach ($offsets as $offset) {
                    $body .= pack('J', $offset);
                }
            }
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds a Fetch response (api key 1, v7 - the version this client sends)
     *
     * <pre>
     *   FetchResponse => ThrottleTimeMs ErrorCode SessionId
     *                    [TopicName [Partition ErrorCode HighwaterMarkOffset LastStableOffset
     *                                LogStartOffset [AbortedTransactions] RecordSetSize RecordSet]]
     * </pre>
     *
     * The top-level `ErrorCode` and `SessionId` of version 7 (KIP-227) are 0 by default, which is what a broker
     * answers a session-less request with - the only fetch this client sends today.
     *
     * The last stable offset defaults to the high water mark and the aborted transactions to `null`, which is what
     * a `read_uncommitted` fetch of a partition without transactions is answered with.
     *
     * @param array<string, array<int, array{int, int, string}>> $topics topic => partition =>
     *        [errorCode, highWaterMarkOffset, record set bytes]
     * @param int                                                $throttleTimeMs Milliseconds the broker delayed the
     *        request
     * @param array<string, array<int, array{int, int, list<array{int, int}>|null}>> $transactionState topic =>
     *        partition => [lastStableOffset, logStartOffset, aborted transactions as [producerId, firstOffset]]
     * @param int                                                $sessionErrorCode Top-level error code of version 7
     * @param int                                                $sessionId        Fetch session id of version 7
     * @param array<string, array<int, array{int, int}>>          $currentLeaders   The leader hint of KIP-951
     *        (version 16), as topic => partition => [leaderId, leaderEpoch]: the tagged `current_leader` of a
     *        partition entry, which a broker writes for a partition it refused 6 or 74
     * @param array<int, array{string, int, string|null}>         $nodeEndpoints    The other half of the same
     *        hint, as node id => [host, port, rack]: the tagged `node_endpoints` of the body
     */
    public static function fetch(
        int $correlationId,
        array $topics,
        int $throttleTimeMs = 0,
        array $transactionState = [],
        int $sessionErrorCode = 0,
        int $sessionId = 0,
        array $preferredReadReplicas = [],
        array $currentLeaders = [],
        array $nodeEndpoints = []
    ): string {
        // Version 12 (Kafka 2.7) is the first FLEXIBLE version of this api (KIP-482): compact strings, compact
        // arrays, a COMPACT record set and a tagged-field section at the end of every structure - which is also
        // where the three fields of version 12 would travel, none of which a ZooKeeper-backed broker sends.
        // Version 13 (Kafka 3.1, KIP-516) replaced the topic NAME of every entry with the 16 raw bytes of its
        // id, so this fixture answers with the id {@see self::topicIdOf()} derives from the name - the very one
        // {@see self::metadata()} reports for it, which is how the client resolves it back.
        $body = pack('N', $throttleTimeMs)
            . pack('n', $sessionErrorCode)
            . pack('N', $sessionId)
            . self::compactCount(count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::topicIdOf((string) $topic) . self::compactCount(count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $highWaterMark, $messageSet]) {
                [$lastStableOffset, $logStartOffset, $aborted] =
                    $transactionState[$topic][$partitionId] ?? [$highWaterMark, 0, null];

                $body .= pack('N', $partitionId)
                    . pack('n', $errorCode)
                    . pack('J', $highWaterMark)
                    . pack('J', $lastStableOffset)
                    . pack('J', $logStartOffset)
                    . self::compactAbortedTransactions($aborted)
                    // The preferred read replica of version 11 (Kafka 2.3, KIP-392), between the aborted
                    // transactions and the records; -1 is "read from the leader", which is what a broker
                    // without a `replica.selector.class` answers
                    . pack('N', $preferredReadReplicas[$topic][$partitionId] ?? 0xFFFFFFFF)
                    . self::compactBytes($messageSet)
                    // The tagged `current_leader` of version 12 (tag 1), which a 3.9.2 node fills in from
                    // version 16 on (KIP-951): the node and the epoch the partition is really led with
                    . self::currentLeaderTag($currentLeaders[$topic][$partitionId] ?? null);
            }
            $body .= self::tagBuffer();
        }
        // The `forgotten_topics_data` of a request has no counterpart here; what closes the body is its section -
        // which is where the `node_endpoints` of version 16 travels, as the tag 0 of the body (KIP-951)
        return self::flexible($correlationId, $body . self::nodeEndpointsTag($nodeEndpoints));
    }

    /**
     * Encodes the tagged-field section of a partition entry that carries the `current_leader` of KIP-951
     *
     * @param array{int, int}|null $currentLeader The leader id and the leader epoch, or null for the empty
     *        section every partition entry that names no leader ends in
     */
    private static function currentLeaderTag(?array $currentLeader): string
    {
        if ($currentLeader === null) {
            return self::tagBuffer();
        }

        [$leaderId, $leaderEpoch] = $currentLeader;
        $value = pack('N', $leaderId) . pack('N', $leaderEpoch) . self::tagBuffer();

        return self::unsignedVarint(1) . self::unsignedVarint(1) . self::unsignedVarint(strlen($value)) . $value;
    }

    /**
     * Encodes the tagged-field section of the body that carries the `node_endpoints` of KIP-951 (Fetch v16)
     *
     * @param array<int, array{string, int, string|null}> $nodeEndpoints node id => [host, port, rack]
     */
    private static function nodeEndpointsTag(array $nodeEndpoints): string
    {
        if ($nodeEndpoints === []) {
            return self::tagBuffer();
        }

        $value = self::compactCount(count($nodeEndpoints));
        foreach ($nodeEndpoints as $nodeId => [$host, $port, $rack]) {
            $value .= pack('N', $nodeId)
                . self::compactString($host)
                . pack('N', $port)
                . ($rack === null ? self::unsignedVarint(0) : self::compactString($rack))
                . self::tagBuffer();
        }

        return self::unsignedVarint(1) . self::unsignedVarint(0) . self::unsignedVarint(strlen($value)) . $value;
    }

    /**
     * Returns the topic id this fixture gives a topic: 16 stable bytes derived from its name (KIP-516)
     *
     * A broker from Kafka 2.8 on has a real id for every topic, and from **Fetch v13** (Kafka 3.1) an api may
     * name a topic by nothing else, so the metadata answer and the fetch answer of this fixture have to agree on
     * one - this is it.
     */
    public static function topicIdOf(string $topic): string
    {
        return substr(md5($topic, true), 0, 16);
    }

    /**
     * Encodes the nullable `aborted_transactions` array of a FLEXIBLE Fetch answer: a compact count, `0` is null
     *
     * @param list<array{0: int, 1: int}>|null $abortedTransactions Producer id and first offset of every entry
     */
    private static function compactAbortedTransactions(?array $abortedTransactions): string
    {
        if ($abortedTransactions === null) {
            return self::unsignedVarint(0);
        }

        $bytes = self::compactCount(count($abortedTransactions));
        foreach ($abortedTransactions as [$producerId, $firstOffset]) {
            $bytes .= pack('J', $producerId) . pack('J', $firstOffset) . self::tagBuffer();
        }

        return $bytes;
    }

    /**
     * Builds a Fetch response of the versions 1 to 3, whose partition entries carry none of the fields of 0.11
     *
     * @param array<string, array<int, array{int, int, string}>> $topics topic => partition =>
     *        [errorCode, highWaterMarkOffset, message set bytes]
     */
    public static function fetchV3(int $correlationId, array $topics, int $throttleTimeMs = 0): string
    {
        $body = pack('N', $throttleTimeMs) . pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $highWaterMark, $messageSet]) {
                $body .= pack('N', $partitionId)
                    . pack('n', $errorCode)
                    . pack('J', $highWaterMark)
                    . pack('N', strlen($messageSet))
                    . $messageSet;
            }
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds a Fetch response of version 0, i.e. the version 1 to 3 answer without the leading `ThrottleTimeMs`
     *
     * @param array<string, array<int, array{int, int, string}>> $topics topic => partition =>
     *        [errorCode, highWaterMarkOffset, message set bytes]
     */
    public static function fetchV0(int $correlationId, array $topics): string
    {
        $frame = self::fetchV3($correlationId, $topics);

        return self::of($correlationId, substr($frame, 12));
    }

    /**
     * Encodes the nullable aborted-transactions array of a Fetch v4/v5 partition, `null` as the element count -1
     *
     * @param list<array{int, int}>|null $abortedTransactions Producer id and first offset of every transaction
     */
    private static function abortedTransactions(?array $abortedTransactions): string
    {
        if ($abortedTransactions === null) {
            return pack('N', -1);
        }

        $body = pack('N', count($abortedTransactions));
        foreach ($abortedTransactions as [$producerId, $firstOffset]) {
            $body .= pack('J', $producerId) . pack('J', $firstOffset);
        }

        return $body;
    }

    /**
     * Builds an OffsetCommit response (api key 8, v8 - the flexible version this client sends)
     *
     * The versions 0, 1 and 2 share one response format, version 3 (KIP-124) put the throttle time in front of it
     * and version 8 (KIP-482, Kafka 2.4) writes the very same fields with the compact types and a tagged-field
     * section per structure.
     *
     * @param array<string, array<int, int>> $topics topic => partition => error code
     */
    public static function offsetCommit(int $correlationId, array $topics): string
    {
        $body = pack('N', 0) . self::compactCount(count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::compactString((string) $topic) . self::compactCount(count($partitions));
            foreach ($partitions as $partitionId => $errorCode) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode) . self::tagBuffer();
            }
            $body .= self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds an OffsetFetch response (api key 9, v8 - the version this client sends)
     *
     * v0 and v1 share the response format, v2 appended the group-level error code, v3 (KIP-124) put the throttle
     * time in front of the topics - the answer therefore carries a number at each of its ends - v5 (KIP-320)
     * inserted the `committed_leader_epoch` of every partition between its offset and its metadata, and **v8**
     * (Kafka 3.0) moved the topics and the group-level error code into a `groups` array, one entry per group of
     * the request. This fixture answers the one group it is given, which is what a single-group request gets.
     *
     * @param array<string, array<int, array{int, int, string}|array{int, int, string, int}>> $topics topic =>
     *        partition => [errorCode, offset, metadata] with an optional fourth element, the committed leader
     *        epoch, which defaults to the -1 of an offset that was committed without one
     * @param int    $groupErrorCode The group-level error code, inside the group entry since version 8
     * @param string $groupId        The group this entry answers for (version 8 and above)
     */
    public static function offsetFetch(
        int $correlationId,
        array $topics,
        int $groupErrorCode = 0,
        string $groupId = ''
    ): string {
        return self::offsetFetchOfGroups($correlationId, [$groupId => [$topics, $groupErrorCode]]);
    }

    /**
     * Builds a batched OffsetFetch response (api key 9, v8): one entry per group of the request
     *
     * @param array<string, array{array<string, array<int, array{int, int, string}|array{int, int, string, int}>>,
     *         int}> $groups group id => [topics as {@see self::offsetFetch()} takes them, group-level error code]
     */
    public static function offsetFetchOfGroups(int $correlationId, array $groups): string
    {
        $body = pack('N', 0) . self::compactCount(count($groups));
        foreach ($groups as $groupId => [$topics, $groupErrorCode]) {
            $body .= self::compactString((string) $groupId) . self::compactCount(count($topics));
            foreach ($topics as $topic => $partitions) {
                $body .= self::compactString((string) $topic) . self::compactCount(count($partitions));
                foreach ($partitions as $partitionId => $partition) {
                    [$errorCode, $offset, $metadata] = $partition;
                    $body .= pack('N', $partitionId)
                        . pack('J', $offset)
                        . pack('N', $partition[3] ?? -1)
                        . self::compactString($metadata)
                        . pack('n', $errorCode)
                        . self::tagBuffer();
                }
                $body .= self::tagBuffer();
            }
            $body .= pack('n', $groupErrorCode) . self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a GroupCoordinator response (api key 10, v4 - FindCoordinator in the 0.11 sources)
     *
     * Version 1 surrounded the error code with the `ThrottleTimeMs` of KIP-124 and a nullable `ErrorMessage`, and
     * **version 4** (KIP-699, Kafka 3.0) moved that error code, that message and the three fields of the
     * coordinator into a `coordinators` array with one entry per key of the request. The entry of this fixture
     * carries the empty `error_message` that a 3.9.2 node writes into every version 4 answer, successful or not.
     */
    public static function groupCoordinator(
        int $correlationId,
        int $errorCode,
        int $nodeId = 0,
        string $host = '127.0.0.1',
        int $port = 9092,
        string $key = ''
    ): string {
        $body = pack('N', 0)
            . self::compactCount(1)
            . self::compactString($key)
            . pack('N', $nodeId)
            . self::compactString($host)
            . pack('N', $port)
            . pack('n', $errorCode)
            . self::compactString('')
            . self::tagBuffer();

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a JoinGroup response (api key 11, v9 - the version this client sends)
     *
     * <pre>
     *   JoinGroupResponse => ThrottleTimeMs ErrorCode GenerationId ProtocolType GroupProtocol LeaderId
     *                          SkipAssignment MemberId [Member]
     *     Member => MemberId GroupInstanceId MemberMetadata
     * </pre>
     *
     * Every member entry carries the nullable `group_instance_id` that version 5 added (KIP-345, Kafka 2.3); the
     * `null` of a dynamic member is written, which is what every member of these fixtures is. Version 7 (KIP-559,
     * Kafka 2.5) put the nullable `protocol_type` in front of the protocol name and made the name nullable as
     * well: an answer that reports an error carries `null` in both. **Version 9 (KIP-814, Kafka 3.2) put the
     * single byte of `skip_assignment` between the leader id and the member id**, which is `false` for every
     * answer but the one of a static leader that came back to a group the coordinator did not rebalance.
     *
     * @param array<string, string> $members Metadata of every member, by member id; filled for the leader only
     */
    public static function joinGroup(
        int $correlationId,
        int $errorCode,
        int $generationId = 1,
        ?string $groupProtocol = 'range',
        string $leaderId = '',
        string $memberId = '',
        array $members = [],
        ?string $protocolType = 'consumer',
        bool $skipAssignment = false
    ): string {
        $body = pack('N', 0)
            . pack('n', $errorCode)
            . pack('N', $generationId)
            . self::compactString($protocolType)
            . self::compactString($groupProtocol)
            . self::compactString($leaderId)
            . pack('C', $skipAssignment ? 1 : 0)
            . self::compactString($memberId)
            . self::compactCount(count($members));
        foreach ($members as $member => $metadata) {
            $body .= self::compactString((string) $member)
                . self::compactString(null)
                . self::compactBytes($metadata)
                . self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a SyncGroup response (api key 14, v5 - the version this client sends)
     *
     * Version 5 (KIP-559, Kafka 2.5) put the nullable `protocol_type` and `protocol_name` of the generation
     * between the error code and the assignment; an answer that reports an error carries `null` in both.
     */
    public static function syncGroup(
        int $correlationId,
        int $errorCode,
        string $assignment = '',
        ?string $protocolType = 'consumer',
        ?string $protocolName = 'range'
    ): string {
        $body = pack('N', 0)
            . pack('n', $errorCode)
            . self::compactString($protocolType)
            . self::compactString($protocolName)
            . self::compactBytes($assignment);

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a SyncGroup response of version 4, the frame without the two protocol fields of KIP-559
     *
     * It is what a coordinator answers a caller of {@see \Protocol\Kafka\Client::syncGroup()} that names no
     * protocol, because a version 5 without the pair is refused with 23.
     */
    public static function syncGroupV4(int $correlationId, int $errorCode, string $assignment = ''): string
    {
        return self::flexible($correlationId, pack('N', 0) . pack('n', $errorCode) . self::compactBytes($assignment));
    }

    /**
     * Builds a Heartbeat response (api key 12, v1): the throttle time and the error code
     */
    public static function heartbeat(int $correlationId, int $errorCode): string
    {
        return self::flexible($correlationId, pack('N', 0) . pack('n', $errorCode));
    }

    /**
     * Builds a LeaveGroup response (api key 13, v3 - the version this client sends)
     *
     * <pre>
     *   LeaveGroupResponse => ThrottleTimeMs ErrorCode [MemberId GroupInstanceId ErrorCode]
     * </pre>
     *
     * Version 3 (KIP-345, Kafka 2.4) appended the member array of the batch it answers. Without `$members` the
     * answer carries one entry that repeats the top-level code, which is what a broker sends back to a member that
     * removed itself; the top-level code is 0 then, because the error of a single member belongs to its entry.
     *
     * @param array<string, array{string|null, int}>|null $members Member id => [instance id, error code] of every
     *        entry of the answer, null for the single entry of a member that left by itself
     */
    public static function leaveGroup(int $correlationId, int $errorCode, ?array $members = null): string
    {
        $members ??= ['one-1' => [null, $errorCode]];
        $body     = pack('N', 0)
            . pack('n', $members === [] ? $errorCode : 0)
            . self::compactCount(count($members));
        foreach ($members as $memberId => [$groupInstanceId, $memberErrorCode]) {
            $body .= self::compactString((string) $memberId)
                . self::compactString($groupInstanceId)
                . pack('n', $memberErrorCode)
                . self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds an InitProducerId response (api key 22, **v2** - the version this client sends)
     *
     * <pre>
     *   InitProducerIdResponse => TAG_BUFFER ThrottleTimeMs ErrorCode ProducerId ProducerEpoch TAG_BUFFER
     * </pre>
     *
     * An answer that carries an error carries -1 as the producer id and as the epoch, which is the default here.
     */
    public static function initProducerId(
        int $correlationId,
        int $errorCode = 0,
        int $producerId = -1,
        int $producerEpoch = -1,
        int $throttleTimeMs = 0
    ): string {
        // Version 2 (Kafka 2.4) is flexible: the response header v1 ends in a tag buffer and so does the body,
        // while the four fields between them are the ones of every version of this api
        $body = "\x00"
            . pack('N', $throttleTimeMs)
            . pack('n', $errorCode)
            . pack('J', $producerId)
            . pack('n', $producerEpoch)
            . "\x00";

        return self::of($correlationId, $body);
    }

    /**
     * Builds a ListGroups response (api key 16, v5 - the version this client sends)
     *
     * <pre>
     *   ListGroupsResponse => ThrottleTimeMs ErrorCode [GroupId ProtocolType GroupState GroupType]
     * </pre>
     *
     * Version 4 (KIP-518, Kafka 2.6) appended the state of the group to every entry and version 5 (KIP-848,
     * Kafka 3.8) its type; a protocol type given as a plain string is answered with the state `Stable` and the
     * type `classic`, the pair `[protocolType, state]` names the first two and the triple
     * `[protocolType, state, type]` all three.
     *
     * @param array<string, string|array{string, string}|array{string, string, string}> $groups Protocol type -
     *        or protocol type and state, or all three - of every group the answering broker coordinates, by
     *        group id
     */
    public static function listGroups(int $correlationId, array $groups, int $errorCode = 0): string
    {
        $body = pack('N', 0) . pack('n', $errorCode) . self::compactCount(count($groups));
        foreach ($groups as $groupId => $group) {
            $entry = is_array($group) ? $group : [$group, 'Stable'];
            [$protocolType, $groupState] = $entry;
            $body .= self::compactString((string) $groupId)
                . self::compactString($protocolType)
                . self::compactString($groupState)
                . self::compactString($entry[2] ?? 'classic')
                . self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a ListGroups response of version 4, the frame whose entries carry a state and no type (below KIP-848)
     *
     * @param array<string, string|array{string, string}> $groups Protocol type - or protocol type and state - of
     *        every group the answering broker coordinates, by group id
     */
    public static function listGroupsV4(int $correlationId, array $groups, int $errorCode = 0): string
    {
        $body = pack('N', 0) . pack('n', $errorCode) . self::compactCount(count($groups));
        foreach ($groups as $groupId => $group) {
            [$protocolType, $groupState] = is_array($group) ? $group : [$group, 'Stable'];
            $body .= self::compactString((string) $groupId)
                . self::compactString($protocolType)
                . self::compactString($groupState)
                . self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a ListGroups response of version 3, the frame whose entries carry no state (below KIP-518)
     *
     * @param array<string, string> $groups Protocol type of every group the answering broker coordinates, by id
     */
    public static function listGroupsV3(int $correlationId, array $groups, int $errorCode = 0): string
    {
        $body = pack('N', 0) . pack('n', $errorCode) . self::compactCount(count($groups));
        foreach ($groups as $groupId => $protocolType) {
            $body .= self::compactString((string) $groupId) . self::compactString($protocolType) . self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a DescribeGroups response (api key 15, v6)
     *
     * <pre>
     *   DescribeGroupsResponse => ThrottleTimeMs [ErrorCode ErrorMessage GroupId State ProtocolType Protocol [Member]
     *                             AuthorizedOperations]
     *     Member => MemberId GroupInstanceId ClientId ClientHost MemberMetadata MemberAssignment
     * </pre>
     *
     * There is no error code for the whole request: every group carries its own, and an entry that has one is
     * otherwise empty, exactly as a broker that is not the coordinator answers it. The `error_message` of version 6
     * (KIP-1043, Kafka 4.0) is the optional sixth element of an entry, null when it is left out.
     *
     * @param array<string, array{0: int, 1: string, 2: string, 3: string, 4: array<string, array{string, string}>, 5?: string|null}> $groups
     *        group id => [errorCode, state, protocolType, protocol, member id => [metadata, assignment], errorMessage]
     */
    public static function describeGroups(int $correlationId, array $groups): string
    {
        $body = pack('N', 0) . self::compactCount(count($groups));
        foreach ($groups as $groupId => $group) {
            [$errorCode, $state, $protocolType, $protocol, $members] = $group;
            $body .= pack('n', $errorCode)
                . self::compactString($group[5] ?? null)
                . self::compactString((string) $groupId)
                . self::compactString($state)
                . self::compactString($protocolType)
                . self::compactString($protocol)
                . self::compactCount(count($members));
            foreach ($members as $memberId => [$metadata, $assignment]) {
                $body .= self::compactString((string) $memberId)
                    // the `group_instance_id` of version 4 (KIP-345): null, a dynamic member
                    . self::compactString(null)
                    . self::compactString('test')
                    . self::compactString('/172.18.0.1')
                    . self::compactBytes($metadata)
                    . self::compactBytes($assignment)
                    . self::tagBuffer();
            }
            // `authorized_operations` of version 3 (KIP-430, Kafka 2.3): Integer.MIN_VALUE, the value of an answer
            // whose request left `include_authorized_operations` at false
            $body .= pack('N', 0x80000000) . self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Builds a ConsumerGroupHeartbeat response (api key 68, v0 - the new consumer protocol of KIP-848)
     *
     * <pre>
     *   ConsumerGroupHeartbeatResponse => ThrottleTimeMs ErrorCode ErrorMessage MemberId MemberEpoch
     *                                     HeartbeatIntervalMs Assignment
     * </pre>
     *
     * `$assignment` is the field that carries the whole reconciliation: **null** is the `ff` of "nothing changed
     * since the last answer" and an array - even an empty one - is the `01` of a structure that follows, i.e. the
     * partitions this member may own now. The map is `raw topic id => list of partitions`, exactly as
     * {@see \Protocol\Kafka\Protocol\Data\ConsumerGroupHeartbeatAssignment::partitionsByTopicId()} answers it.
     *
     * @param array<string, list<int>>|null $assignment Partitions of the member, null for "unchanged"
     */
    public static function consumerGroupHeartbeat(
        int $correlationId,
        int $errorCode = 0,
        ?string $memberId = null,
        int $memberEpoch = 1,
        int $heartbeatIntervalMs = 5000,
        ?array $assignment = null,
        ?string $errorMessage = null
    ): string {
        $body = pack('N', 0)
            . pack('n', $errorCode)
            . self::compactString($errorMessage)
            . self::compactString($memberId)
            . pack('N', $memberEpoch)
            . pack('N', $heartbeatIntervalMs);

        if ($assignment === null) {
            $body .= pack('c', -1);
        } else {
            $body .= pack('c', 1) . self::compactCount(count($assignment));
            foreach ($assignment as $topicId => $partitions) {
                $body .= (string) $topicId
                    . self::compactInt32Array($partitions)
                    . self::tagBuffer();
            }
            $body .= self::tagBuffer();
        }

        return self::flexible($correlationId, $body);
    }

    /**
     * Encodes a non-nullable string: int16 length prefix followed by the content
     */
    private static function string(string $value): string
    {
        return pack('n', strlen($value)) . $value;
    }

    /**
     * Encodes a nullable string: the length -1 for null, otherwise the plain string
     */
    private static function nullableString(?string $value): string
    {
        return $value === null ? pack('n', 0xFFFF) : self::string($value);
    }

    /**
     * Encodes a byte array: int32 length prefix followed by the content
     */
    private static function bytes(string $value): string
    {
        return pack('N', strlen($value)) . $value;
    }

    /**
     * Encodes an int32 array: int32 length prefix followed by the entries
     *
     * @param list<int> $values
     */
    private static function int32Array(array $values): string
    {
        $buffer = pack('N', count($values));
        foreach ($values as $value) {
            $buffer .= pack('N', $value);
        }

        return $buffer;
    }
}
