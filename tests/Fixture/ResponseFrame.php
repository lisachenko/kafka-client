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
 * Builds the response frames of the 0.8.2.2 APIs, byte for byte as the specification describes them.
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
 * @see docs/protocol/0.8.2.md
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
     * Builds a Metadata response (api key 3, v0)
     *
     * <pre>
     *   MetadataResponse => [Broker][TopicMetadata]
     *     Broker            => NodeId int32 Host string Port int32
     *     TopicMetadata     => TopicErrorCode int16 TopicName string [PartitionMetadata]
     *     PartitionMetadata => PartitionErrorCode int16 PartitionId int32 Leader int32 Replicas [int32] Isr [int32]
     * </pre>
     *
     * @param list<array{int, string, int}>                          $brokers nodeId, host, port
     * @param array<string, array<int, int>>                         $topics  topic => partition => leader node id
     * @param array<string, int>                                     $topicErrorCodes  Error code of a topic, if any
     * @param array<string, array<int, int>>                         $partitionErrorCodes Error code of a partition
     */
    public static function metadata(
        int $correlationId,
        array $brokers,
        array $topics = [],
        array $topicErrorCodes = [],
        array $partitionErrorCodes = []
    ): string {
        $body = pack('N', count($brokers));
        foreach ($brokers as [$nodeId, $host, $port]) {
            $body .= pack('N', $nodeId) . self::string($host) . pack('N', $port);
        }

        $body .= pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= pack('n', $topicErrorCodes[$topic] ?? 0) . self::string((string) $topic);
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
     * Builds a Produce response (api key 0, v0)
     *
     * <pre>
     *   ProduceResponse => [TopicName [Partition ErrorCode Offset]]
     * </pre>
     *
     * @param array<string, array<int, array{int, int}>> $topics topic => partition => [errorCode, baseOffset]
     */
    public static function produce(int $correlationId, array $topics): string
    {
        $body = pack('N', count($topics));
        foreach ($topics as $topic => $partitions) {
            $body .= self::string((string) $topic) . pack('N', count($partitions));
            foreach ($partitions as $partitionId => [$errorCode, $baseOffset]) {
                $body .= pack('N', $partitionId) . pack('n', $errorCode) . pack('J', $baseOffset);
            }
        }

        return self::of($correlationId, $body);
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
     * Builds a Fetch response (api key 1, v0)
     *
     * <pre>
     *   FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
     * </pre>
     *
     * @param array<string, array<int, array{int, int, string}>> $topics topic => partition =>
     *        [errorCode, highWaterMarkOffset, message set bytes]
     */
    public static function fetch(int $correlationId, array $topics): string
    {
        $body = pack('N', count($topics));
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
     * Builds an OffsetCommit response (api key 8, v0 and v1 share the response format)
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
     * Builds an OffsetFetch response (api key 9, v0 and v1 share the response format)
     *
     * @param array<string, array<int, array{int, int, string}>> $topics topic => partition =>
     *        [errorCode, offset, metadata]
     */
    public static function offsetFetch(int $correlationId, array $topics): string
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
     * Encodes a non-nullable string: int16 length prefix followed by the content
     */
    private static function string(string $value): string
    {
        return pack('n', strlen($value)) . $value;
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
