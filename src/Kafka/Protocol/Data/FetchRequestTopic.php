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
 * One topic of a Fetch request
 *
 * <pre>
 *   FetchRequestTopic => TopicName [Partition FetchOffset LogStartOffset MaxBytes]
 *     TopicName => string
 * </pre>
 *
 * Up to version 12 the topic entry names the topic by its **name**, and what a version selects is only the shape
 * of its partition entries, which is what the version constant of this DTO picks in {@see self::partitionClass()},
 * see {@see FetchRequestTopicV0}.
 *
 * **Version 13 (Kafka 3.1, KIP-516) replaces the name with the `topic_id`**: `FetchRequest.json` @ 3.1.2 declares
 * `Topic` as `versions 0-12` and `TopicId` as `13+`, so a version 13 entry is 16 raw bytes instead of a compact
 * string, and a client that does not know the id of a topic can not fetch it at all - it refreshes its metadata
 * first, see {@see \Protocol\Kafka\Common\Cluster::topicIdOf()}. The {@see self::$topic} of this class stays
 * next to the id for the client that filled it in: it is not on the wire of a version 13 frame, and an entry that
 * was **decoded** from one carries the empty name until the id is resolved against the cluster.
 *
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v13)" and "The topic ids of the fetch path
 *      (v13, KIP-516)"
 */
class FetchRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO is packed for
     */
    public const int VERSION = 13;

    /**
     * Name of the topic to fetch from, the empty string in an entry that was decoded from a version 13 frame
     *
     * The field is on the wire in the versions 0 to 12 only.
     */
    public string $topic;

    /**
     * Id of the topic to fetch from, the 16 raw bytes of the `uuid` of KIP-516
     *
     * {@see Uuid::ZERO} is what every version below 13 leaves here and what a caller that names the topic by its
     * name means; a version 13 frame that carries it is refused by the broker with **100** `UnknownTopicId`,
     * because no topic of a cluster ever has the zero id.
     *
     * @since Version 13 of protocol (Kafka 3.1, KIP-516)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Partitions of this topic to fetch from, indexed by the partition id
     *
     * @var array<int, FetchRequestTopicPartition>
     */
    public array $partitions;

    /**
     * @param array<int, FetchRequestTopicPartition> $partitions Partitions to fetch from, indexed by partition id
     */
    public function __construct(string $topic, array $partitions = [], string $topicId = Uuid::ZERO)
    {
        $this->topic      = $topic;
        $this->partitions = $partitions;
        $this->topicId    = $topicId;
    }

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
     * @return class-string<FetchRequestTopicPartition>
     */
    public static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 12 => FetchRequestTopicPartition::class,
            static::VERSION >= 9  => FetchRequestTopicPartitionV9::class,
            static::VERSION >= 5  => FetchRequestTopicPartitionV5::class,
            default               => FetchRequestTopicPartitionV0::class,
        };
    }
}
