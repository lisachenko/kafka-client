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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic of a Fetch response
 *
 * <pre>
 *   FetchResponseTopic => TopicName [Partition ErrorCode HighwaterMarkOffset LastStableOffset LogStartOffset
 *                                    [AbortedTransactions] RecordSetSize RecordSet]
 *     TopicName => string
 * </pre>
 *
 * Up to version 12 the entry names the topic by its **name**, and what a version selects is only the shape of its
 * partition entries, which is what the version constant of this DTO picks in {@see self::partitionClass()}, see
 * {@see FetchResponseTopicV4} and {@see FetchResponseTopicV0}.
 *
 * **Version 13 (Kafka 3.1, KIP-516) replaces the name with the `topic_id`**: `FetchResponse.json` @ 3.1.2
 * declares `Topic` as `versions 0-12` and `TopicId` as `13+`, so an answer names the topics of the request by the
 * very ids the request carried - the name never travels back - and a client resolves them against the map it
 * built the request from, {@see \Protocol\Kafka\Common\Cluster::topicNameById()}. The {@see self::$topic} of a
 * decoded version 13 entry is therefore the empty string.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)" and "The topic ids of the fetch path
 *      (v13, KIP-516)"
 */
class FetchResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO is unpacked from
     */
    public const int VERSION = 13;

    /**
     * Name of the topic that was fetched from, the empty string in an entry decoded from a version 13 answer
     *
     * The field is on the wire in the versions 0 to 12 only.
     */
    public string $topic = '';

    /**
     * Id of the topic that was fetched from, the 16 raw bytes of the `uuid` of KIP-516
     *
     * It is the id the request named, echoed back; {@see Uuid::ZERO} is what every version below 13 leaves here.
     *
     * @since Version 13 of protocol (Kafka 3.1, KIP-516)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Fetch result for each of the requested partitions, indexed by the partition id
     *
     * @var array<int, FetchResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        // KIP-516 replaced the name with the id in version 13; the two never travel together
        $scheme = static::VERSION >= 13
            ? ['topicId' => BinarySchema::TYPE_UUID]
            : ['topic' => BinarySchema::TYPE_STRING];

        $scheme['partitions'] = ['partition' => static::partitionClass()];

        return $scheme;
    }

    /**
     * Returns the class of a partition entry for the version of the API that this DTO belongs to
     *
     * @return class-string<FetchResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 12 => FetchResponsePartition::class,
            static::VERSION >= 11 => FetchResponsePartitionV11::class,
            static::VERSION >= 5  => FetchResponsePartitionV5::class,
            static::VERSION >= 4  => FetchResponsePartitionV4::class,
            default               => FetchResponsePartitionV0::class,
        };
    }
}
