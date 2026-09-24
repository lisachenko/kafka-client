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
 * Produce request Topic DTO
 *
 * <pre>
 *   TopicId [Partition RecordSet]
 *     TopicId => uuid          -- version 13 and above, in place of the name
 *   TopicName [Partition RecordSetSize RecordSet]
 *     TopicName => string      -- versions 0 to 12
 * </pre>
 *
 * **Version 13 (Kafka 4.1, KIP-516) replaces the name of the topic with its `topic_id`**: `ProduceRequest.json` @
 * 4.1.0 declares `Name` as `"versions": "0-12"` and `TopicId` as `"13+"`, "Version 13 replaces topic names with topic
 * IDs (KIP-516). May return UNKNOWN_TOPIC_ID error code." This class is the entry of version 13 - the 16 raw bytes of
 * the id and no name on the wire - and {@see ProduceRequestTopicV12} the one of every version below it. The
 * {@see self::$topic} stays next to the id for the client that filled it in; an entry that was **decoded** from a
 * version 13 frame carries the empty name.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)" and "The topic ids of the produce path (v13,
 *      KIP-516)"
 */
class ProduceRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO is packed for
     */
    public const int VERSION = 13;

    /**
     * The name of the topic to produce to, the empty string in an entry decoded from a version 13 frame
     *
     * The field is on the wire in the versions 0 to 12 only.
     */
    public string $topic = '';

    /**
     * Id of the topic to produce to, the 16 raw bytes of the `uuid` of KIP-516
     *
     * {@see Uuid::ZERO} is what every version below 13 leaves here.
     *
     * @since Version 13 of protocol (Kafka 4.1, KIP-516)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Data for all partitions of this topic, indexed by the partition number
     *
     * @var array<int, ProduceRequestPartition>
     */
    public array $partitions = [];

    /**
     * @param string                              $topic         Name of the topic
     * @param array<int, ProduceRequestPartition> $partitionData Record sets, indexed by the partition number
     * @param string                              $topicId       Id of the topic, the 16 raw bytes of its uuid
     */
    public function __construct(string $topic = '', array $partitionData = [], string $topicId = Uuid::ZERO)
    {
        $this->topic      = $topic;
        $this->partitions = $partitionData;
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
        $scheme['partitions'] = ['partition' => ProduceRequestPartition::class];

        return $scheme;
    }
}
