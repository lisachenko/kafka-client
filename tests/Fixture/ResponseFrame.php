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
 * @see docs/protocol/0.10.2.md
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
     * Builds a Metadata response (api key 3, v2)
     *
     * <pre>
     *   MetadataResponse => [Broker] ClusterId ControllerId [TopicMetadata]
     *     Broker            => NodeId int32 Host string Port int32 Rack nullable string
     *     ClusterId         => nullable string
     *     ControllerId      => int32
     *     TopicMetadata     => TopicErrorCode int16 TopicName string IsInternal boolean [PartitionMetadata]
     *     PartitionMetadata => PartitionErrorCode int16 PartitionId int32 Leader int32 Replicas [int32] Isr [int32]
     * </pre>
     *
     * The first broker of the list is the controller unless `$controllerId` says otherwise, and no broker declares
     * a rack - the answer of the container of `docker-compose.yml`, which runs a single broker without
     * `broker.rack`. A topic counts as internal when its name is in `$internalTopics`, i.e. `__consumer_offsets`
     * and nothing else on a 0.10.2.2 cluster.
     *
     * @param list<array{int, string, int}>  $brokers             nodeId, host, port
     * @param array<string, array<int, int>> $topics              topic => partition => leader node id
     * @param array<string, int>             $topicErrorCodes     Error code of a topic, if any
     * @param array<string, array<int, int>> $partitionErrorCodes Error code of a partition
     * @param list<string>                   $internalTopics      Topics to flag with `is_internal`
     * @param int|null                       $controllerId        Controller of the cluster, -1 while it elects one
     */
    public static function metadata(
        int $correlationId,
        array $brokers,
        array $topics = [],
        array $topicErrorCodes = [],
        array $partitionErrorCodes = [],
        array $internalTopics = [],
        ?int $controllerId = null
    ): string {
        $body = pack('N', count($brokers));
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
                    . self::int32Array($replicas)
                    . self::int32Array($replicas);
            }
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds a Produce response (api key 0, v1)
     *
     * <pre>
     *   ProduceResponse => [TopicName [Partition ErrorCode Offset]] ThrottleTime
     * </pre>
     *
     * @param array<string, array<int, array{int, int}>> $topics       topic => partition => [errorCode, baseOffset]
     * @param int                                        $throttleTime Milliseconds the broker delayed the request
     */
    public static function produce(int $correlationId, array $topics, int $throttleTime = 0): string
    {
        $body = pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $baseOffset]) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode) . pack('J', $baseOffset);
            }
        }
        // The throttle time of v1 closes the response, the opposite end from where the Fetch API puts it
        $body .= pack('N', $throttleTime);

        return self::of($correlationId, $body);
    }

    /**
     * Builds a Produce response of version 0, i.e. the same answer without the trailing `ThrottleTime`
     *
     * @param array<string, array<int, array{int, int}>> $topics topic => partition => [errorCode, baseOffset]
     */
    public static function produceV0(int $correlationId, array $topics): string
    {
        $frame = self::produce($correlationId, $topics);

        return self::of($correlationId, substr($frame, 8, -4));
    }

    /**
     * Builds an Offsets (ListOffset) response (api key 2, v0)
     *
     * <pre>
     *   OffsetResponse => [TopicName [PartitionOffsets]]
     *     PartitionOffsets => Partition ErrorCode [Offset]
     * </pre>
     *
     * @param array<string, array<int, array{int, list<int>}>> $topics topic => partition => [errorCode, offsets]
     */
    public static function offsets(int $correlationId, array $topics): string
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
     * Builds a Fetch response (api key 1, v1)
     *
     * <pre>
     *   FetchResponse => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize
     *                                               MessageSet]]
     * </pre>
     *
     * @param array<string, array<int, array{int, int, string}>> $topics topic => partition =>
     *        [errorCode, highWaterMarkOffset, message set bytes]
     * @param int                                                $throttleTimeMs Milliseconds the broker delayed the
     *        request
     */
    public static function fetch(int $correlationId, array $topics, int $throttleTimeMs = 0): string
    {
        // The throttle time of v1 opens the response, before the topics array
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
     * Builds a Fetch response of version 0, i.e. the same answer without the leading `ThrottleTimeMs`
     *
     * @param array<string, array<int, array{int, int, string}>> $topics topic => partition =>
     *        [errorCode, highWaterMarkOffset, message set bytes]
     */
    public static function fetchV0(int $correlationId, array $topics): string
    {
        $frame = self::fetch($correlationId, $topics);

        return self::of($correlationId, substr($frame, 12));
    }

    /**
     * Builds an OffsetCommit response (api key 8, the versions 0, 1 and 2 share the response format)
     *
     * @param array<string, array<int, int>> $topics topic => partition => error code
     */
    public static function offsetCommit(int $correlationId, array $topics): string
    {
        $body = pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => $errorCode) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode);
            }
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds an OffsetFetch response (api key 9; v0 and v1 share the response format, v2 appends a group error)
     *
     * @param array<string, array<int, array{int, int, string}>> $topics topic => partition =>
     *        [errorCode, offset, metadata]
     * @param int|null $groupErrorCode The group-level error code of version 2, null for a version 0 or 1 answer
     */
    public static function offsetFetch(int $correlationId, array $topics, ?int $groupErrorCode = 0): string
    {
        $body = pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $offset, $metadata]) {
                $body .= pack('N', $partitionId)
                    . pack('J', $offset)
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
     * Builds a GroupCoordinator response (api key 10, v0, ConsumerMetadata in Kafka 0.8.2)
     */
    public static function groupCoordinator(
        int $correlationId,
        int $errorCode,
        int $nodeId = 0,
        string $host = '127.0.0.1',
        int $port = 9092
    ): string {
        $body = pack('n', $errorCode) . pack('N', $nodeId) . self::string($host) . pack('N', $port);

        return self::of($correlationId, $body);
    }

    /**
     * Builds a JoinGroup response (api key 11, v0)
     *
     * <pre>
     *   JoinGroupResponse => ErrorCode GenerationId GroupProtocol LeaderId MemberId [MemberId MemberMetadata]
     * </pre>
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
        $body = pack('n', $errorCode)
            . pack('N', $generationId)
            . self::string($groupProtocol)
            . self::string($leaderId)
            . self::string($memberId)
            . pack('N', count($members));
        foreach ($members as $member => $metadata) {
            $body .= self::string((string) $member) . self::bytes($metadata);
        }

        return self::of($correlationId, $body);
    }

    /**
     * Builds a SyncGroup response (api key 14, v0), which has no throttle time before Kafka 0.10.1
     */
    public static function syncGroup(int $correlationId, int $errorCode, string $assignment = ''): string
    {
        return self::of($correlationId, pack('n', $errorCode) . self::bytes($assignment));
    }

    /**
     * Builds a Heartbeat response (api key 12, v0), whose whole body is the error code
     */
    public static function heartbeat(int $correlationId, int $errorCode): string
    {
        return self::of($correlationId, pack('n', $errorCode));
    }

    /**
     * Builds a LeaveGroup response (api key 13, v0), whose whole body is the error code
     */
    public static function leaveGroup(int $correlationId, int $errorCode): string
    {
        return self::of($correlationId, pack('n', $errorCode));
    }

    /**
     * Encodes a non-nullable string: int16 length prefix followed by the content
     */
    private static function string(string $value): string
    {
        return pack('n', strlen($value)) . $value;
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
