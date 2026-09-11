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
 * @see docs/protocol/2.8.md
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
        int $clusterAuthorizedOperations = self::NOT_REQUESTED
    ): string {
        // The throttle time of version 3 opens the body, in front of the brokers
        $body = pack('N', 0) . pack('N', count($brokers));
        foreach ($brokers as [$nodeId, $host, $port]) {
            // The rack of the broker, null for a cluster that is not rack aware
            $body .= pack('N', $nodeId) . self::string($host) . pack('N', $port) . pack('n', 0xFFFF);
        }

        $body .= self::string(self::CLUSTER_ID);
        $body .= pack('N', $controllerId ?? $brokers[0][0] ?? -1);

        $body .= pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= pack('n', $topicErrorCodes[$topic] ?? 0) . self::string((string) $topic);
            $body .= pack('C', in_array((string) $topic, $internalTopics, true) ? 1 : 0);
            $body .= pack('N', count($partitions));
            foreach ($partitions as $partitionId => $leader) {
                $replicas = $leader < 0 ? [] : [$leader];
                $body .= pack('n', $partitionErrorCodes[$topic][$partitionId] ?? 0)
                    . pack('N', $partitionId)
                    . pack('N', $leader)
                    // The leader epoch of version 7 (Kafka 2.1, KIP-320), behind the leader id
                    . pack('N', $leaderEpochs[$topic][$partitionId] ?? 0)
                    . self::int32Array($replicas)
                    . self::int32Array($replicas)
                    . self::int32Array($offlineReplicas[$topic][$partitionId] ?? []);
            }

            // The `topic_authorized_operations` bitfield of version 8 (Kafka 2.3, KIP-430), behind the
            // partitions; Integer.MIN_VALUE is what a broker writes when the request did not ask for it
            $body .= pack('N', $topicAuthorizedOperations[$topic] ?? self::NOT_REQUESTED);
        }

        // And the `cluster_authorized_operations` of the same version, at the very end of the frame
        $body .= pack('N', $clusterAuthorizedOperations);

        return self::of($correlationId, $body);
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
        // The throttle time of v1 closes the response, the opposite end from where the Fetch API puts it
        $body = self::produceTopics($topics, $logAppendTime, $logStartOffsets, $recordErrors)
            . pack('N', $throttleTime);

        return self::of($correlationId, $body);
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
        ?array $recordErrors = null
    ): string {
        $body = pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
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
                $body .= pack('N', count($errors));
                foreach ($errors as $batchIndex => $batchMessage) {
                    $body .= pack('N', $batchIndex) . self::nullableString($batchMessage);
                }
                $body .= self::nullableString($message);
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
        $body = pack('N', 0) . pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $timestamp, $offset]) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode) . pack('J', $timestamp) . pack('J', $offset)
                    // The leader epoch of version 4 (Kafka 2.1, KIP-320), behind the offset
                    . pack('N', $leaderEpochs[$topic][$partitionId] ?? 0xFFFFFFFF);
            }
        }

        return self::of($correlationId, $body);
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
     */
    public static function fetch(
        int $correlationId,
        array $topics,
        int $throttleTimeMs = 0,
        array $transactionState = [],
        int $sessionErrorCode = 0,
        int $sessionId = 0,
        array $preferredReadReplicas = []
    ): string {
        // The throttle time of v1 opens the response, before the topics array; the session error code and the
        // session id of v7 sit between the two
        $body = pack('N', $throttleTimeMs)
            . pack('n', $sessionErrorCode)
            . pack('N', $sessionId)
            . pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $highWaterMark, $messageSet]) {
                [$lastStableOffset, $logStartOffset, $aborted] =
                    $transactionState[$topic][$partitionId] ?? [$highWaterMark, 0, null];

                $body .= pack('N', $partitionId)
                    . pack('n', $errorCode)
                    . pack('J', $highWaterMark)
                    . pack('J', $lastStableOffset)
                    . pack('J', $logStartOffset)
                    . self::abortedTransactions($aborted)
                    // The preferred read replica of version 11 (Kafka 2.3, KIP-392), between the aborted
                    // transactions and the records; -1 is "read from the leader", which is what a broker
                    // without a `replica.selector.class` answers
                    . pack('N', $preferredReadReplicas[$topic][$partitionId] ?? 0xFFFFFFFF)
                    . pack('N', strlen($messageSet))
                    . $messageSet;
            }
        }

        return self::of($correlationId, $body);
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
     * Builds an OffsetCommit response (api key 8, v3 - the version this client sends)
     *
     * The versions 0, 1 and 2 share one response format, and version 3 (KIP-124) put the throttle time in front
     * of it.
     *
     * @param array<string, array<int, int>> $topics topic => partition => error code
     */
    public static function offsetCommit(int $correlationId, array $topics): string
    {
        $body = pack('N', 0) . pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => $errorCode) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode);
            }
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds an OffsetFetch response (api key 9, v5 - the version this client sends)
     *
     * v0 and v1 share the response format, v2 appended the group-level error code, v3 (KIP-124) put the throttle
     * time in front of the topics - the answer therefore carries a number at each of its ends - and v5 (KIP-320)
     * inserted the `committed_leader_epoch` of every partition between its offset and its metadata.
     *
     * @param array<string, array<int, array{int, int, string}|array{int, int, string, int}>> $topics topic =>
     *        partition => [errorCode, offset, metadata] with an optional fourth element, the committed leader
     *        epoch, which defaults to the -1 of an offset that was committed without one
     * @param int|null $groupErrorCode The group-level error code of version 2 and above, null for v0 or v1
     */
    public static function offsetFetch(int $correlationId, array $topics, ?int $groupErrorCode = 0): string
    {
        $body = pack('N', 0) . pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => $partition) {
                [$errorCode, $offset, $metadata] = $partition;
                $body .= pack('N', $partitionId)
                    . pack('J', $offset)
                    . pack('N', $partition[3] ?? -1)
                    . self::string($metadata)
                    . pack('n', $errorCode);
            }
        }
        if ($groupErrorCode !== null) {
            $body .= pack('n', $groupErrorCode);
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds a GroupCoordinator response (api key 10, v1 - FindCoordinator in the 0.11 sources)
     *
     * Version 1 surrounds the error code with the `ThrottleTimeMs` of KIP-124 and a nullable `ErrorMessage`; a
     * 0.11.0.3 broker leaves that message null in every answer, which is what this fixture reproduces.
     */
    public static function groupCoordinator(
        int $correlationId,
        int $errorCode,
        int $nodeId = 0,
        string $host = '127.0.0.1',
        int $port = 9092
    ): string {
        $body = pack('N', 0)
            . pack('n', $errorCode)
            . pack('n', 0xFFFF)
            . pack('N', $nodeId)
            . self::string($host)
            . pack('N', $port);

        return self::of($correlationId, $body);
    }

    /**
     * Builds a JoinGroup response (api key 11, v5 - the version this client sends)
     *
     * <pre>
     *   JoinGroupResponse => ThrottleTimeMs ErrorCode GenerationId GroupProtocol LeaderId MemberId [Member]
     *     Member => MemberId GroupInstanceId MemberMetadata
     * </pre>
     *
     * Every member entry carries the nullable `group_instance_id` that version 5 added (KIP-345, Kafka 2.3); the
     * `null` of a dynamic member is written, which is what every member of these fixtures is.
     *
     * @param array<string, string> $members Metadata of every member, by member id; filled for the leader only
     */
    public static function joinGroup(
        int $correlationId,
        int $errorCode,
        int $generationId = 1,
        string $groupProtocol = 'range',
        string $leaderId = '',
        string $memberId = '',
        array $members = []
    ): string {
        $body = pack('N', 0)
            . pack('n', $errorCode)
            . pack('N', $generationId)
            . self::string($groupProtocol)
            . self::string($leaderId)
            . self::string($memberId)
            . pack('N', count($members));
        foreach ($members as $member => $metadata) {
            $body .= self::string((string) $member) . self::nullableString(null) . self::bytes($metadata);
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds a SyncGroup response (api key 14, v1), whose throttle time arrived with Kafka 0.11 (KIP-124)
     */
    public static function syncGroup(int $correlationId, int $errorCode, string $assignment = ''): string
    {
        return self::of($correlationId, pack('N', 0) . pack('n', $errorCode) . self::bytes($assignment));
    }

    /**
     * Builds a Heartbeat response (api key 12, v1): the throttle time and the error code
     */
    public static function heartbeat(int $correlationId, int $errorCode): string
    {
        return self::of($correlationId, pack('N', 0) . pack('n', $errorCode));
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
            . pack('N', count($members));
        foreach ($members as $memberId => [$groupInstanceId, $memberErrorCode]) {
            $body .= self::string((string) $memberId)
                . self::nullableString($groupInstanceId)
                . pack('n', $memberErrorCode);
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds an InitProducerId response (api key 22, v0)
     *
     * <pre>
     *   InitProducerIdResponse => ThrottleTimeMs ErrorCode ProducerId ProducerEpoch
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
        $body = pack('N', $throttleTimeMs)
            . pack('n', $errorCode)
            . pack('J', $producerId)
            . pack('n', $producerEpoch);

        return self::of($correlationId, $body);
    }

    /**
     * Builds a ListGroups response (api key 16, v1)
     *
     * <pre>
     *   ListGroupsResponse => ThrottleTimeMs ErrorCode [GroupId ProtocolType]
     * </pre>
     *
     * @param array<string, string> $groups Protocol type of every group the answering broker coordinates, by id
     */
    public static function listGroups(int $correlationId, array $groups, int $errorCode = 0): string
    {
        $body = pack('N', 0) . pack('n', $errorCode) . pack('N', count($groups));
        foreach ($groups as $groupId => $protocolType) {
            $body .= self::string((string) $groupId) . self::string($protocolType);
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds a DescribeGroups response (api key 15, v1)
     *
     * <pre>
     *   DescribeGroupsResponse => ThrottleTimeMs [ErrorCode GroupId State ProtocolType Protocol [Member]]
     *     Member => MemberId ClientId ClientHost MemberMetadata MemberAssignment
     * </pre>
     *
     * There is no error code for the whole request: every group carries its own, and an entry that has one is
     * otherwise empty, exactly as a broker that is not the coordinator answers it.
     *
     * @param array<string, array{int, string, string, string, array<string, array{string, string}>}> $groups
     *        group id => [errorCode, state, protocolType, protocol, member id => [metadata, assignment]]
     */
    public static function describeGroups(int $correlationId, array $groups): string
    {
        $body = pack('N', 0) . pack('N', count($groups));
        foreach ($groups as $groupId => [$errorCode, $state, $protocolType, $protocol, $members]) {
            $body .= pack('n', $errorCode)
                . self::string((string) $groupId)
                . self::string($state)
                . self::string($protocolType)
                . self::string($protocol)
                . pack('N', count($members));
            foreach ($members as $memberId => [$metadata, $assignment]) {
                $body .= self::string((string) $memberId)
                    . self::string('test')
                    . self::string('/172.18.0.1')
                    . self::bytes($metadata)
                    . self::bytes($assignment);
            }
            // `authorized_operations` of version 3 (KIP-430, Kafka 2.3): Integer.MIN_VALUE, the value of an answer
            // whose request left `include_authorized_operations` at false
            $body .= pack('N', 0x80000000);
        }

        return self::of($correlationId, $body);
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
