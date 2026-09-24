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
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Produce response Topic DTO
 *
 * <pre>
 *   TopicName [Partition ErrorCode Offset LogAppendTime LogStartOffset]
 *     TopicName => string
 * </pre>
 *
 * The topic entry itself never changed; what a version selects is the shape of its partition entries - a version 0
 * or 1 answer carries no `LogAppendTime` ({@see ProduceResponseTopicV0}), a version 2, 3 or 4 answer no
 * `LogStartOffset` ({@see ProduceResponseTopicV2}) - which is what the version constant of this DTO picks in
 * {@see self::partitionClass()}. The versions 3 and 4 changed nothing about the answer at all; version 5 (Kafka
 * 1.0) appended the `LogStartOffset` to every partition entry, version 8 (Kafka 2.4) the record errors of
 * KIP-467 ({@see ProduceResponseTopicV8} is the entry of the versions 8 and 9) and version 10 (Kafka 3.7,
 * KIP-951) the tagged `current_leader` ({@see ProduceResponseTopicV10} is the entry of the versions 10 to 12).
 *
 * **Version 13 (Kafka 4.1, KIP-516) is the first that changed the topic entry itself**: `ProduceResponse.json` @
 * 4.1.0 declares `Name` as `"versions": "0-12"` and `TopicId` as `"13+"`, so the answer names every topic by its
 * id, and this class is that entry. Its {@see self::$topic} is the empty string on the wire; the client maps the
 * id back to the name it produced to, see {@see \Protocol\Kafka\Client::produce()}.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)", "The leader discovery of KIP-951 (v10)" and
 *      "The topic ids of the produce path (v13, KIP-516)"
 */
class ProduceResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO is unpacked from
     */
    public const int VERSION = 13;

    /**
     * The name of the topic, the empty string in an entry of a version 13 answer
     */
    public string $topic = '';

    /**
     * Id of the topic, the 16 raw bytes of the `uuid` of KIP-516, {@see Uuid::ZERO} below version 13
     *
     * @since Version 13 of protocol (Kafka 4.1, KIP-516)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Result for all partitions of this topic, indexed by the partition number
     *
     * @var array<int, ProduceResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        // KIP-516 replaced the name with the id in version 13
        $scheme = static::VERSION >= 13
            ? ['topicId' => BinarySchema::TYPE_UUID]
            : ['topic' => BinarySchema::TYPE_STRING];
        $scheme['partitions'] = ['partition' => static::partitionClass()];

        return $scheme;
    }

    /**
     * Returns the class of a partition entry for the version of the API that this DTO belongs to
     *
     * @return class-string<ProduceResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 10 => ProduceResponsePartition::class,
            static::VERSION >= 8 => ProduceResponsePartitionV8::class,
            static::VERSION >= 5 => ProduceResponsePartitionV5::class,
            static::VERSION >= 2 => ProduceResponsePartitionV2::class,
            default              => ProduceResponsePartitionV0::class,
        };
    }
}
