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

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * Partitions that the leader of a group hands to one of its members, the `protocol_type = "consumer"` payload of
 * the SyncGroup request and response.
 *
 * <pre>
 *   MemberAssignment => Version [Topic [Partition]] UserData
 *     Version   => int16
 *     Topic     => string
 *     Partition => int32
 *     UserData  => bytes
 * </pre>
 *
 * The leader packs one of these per member into the `member_assignment` field of its SyncGroup request and every
 * member gets its own back in the SyncGroup response. A member that the assignor left without partitions receives
 * a structure with an empty topic array, not an empty byte array - this is what a Kafka 0.10.2.2 broker relays when
 * a group has more members than the subscribed topics have partitions.
 *
 * Kafka 0.10.2.2 knows exactly one version, {@see MemberAssignment::VERSION}, and the built-in assignors send the
 * empty `UserData` that the Java client sends.
 *
 * @see docs/protocol/0.10.2.md, section "Consumer group protocol (protocol_type = consumer)"
 * @see \Protocol\Kafka\Consumer\PartitionAssignorInterface::assign()
 */
class MemberAssignment implements BinarySchemaInterface
{
    /**
     * Version of the consumer group protocol that Kafka 0.10.2.2 speaks
     */
    public const int VERSION = 0;

    /**
     * Version of the structure, `ConsumerProtocol.CONSUMER_PROTOCOL_V0` in the Java client
     */
    public int $version;

    /**
     * Assigned partitions, indexed by the topic they belong to
     *
     * @var array<string, PartitionsForTopic>
     */
    public array $topicPartitions = [];

    /**
     * Opaque data of the assignor, null for the `bytes` value -1 that the protocol defines as null
     */
    public ?string $userData;

    /**
     * An entry of the assignment is either the list of partition ids of that topic or an already built DTO.
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Assigned partitions per topic
     * @param int                                         $version         Version of the structure, 0 in 0.10.2.2
     * @param string|null                                 $userData        Data of the assignor for the member
     */
    public function __construct(array $topicPartitions = [], int $version = self::VERSION, ?string $userData = '')
    {
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopicPartitions[$topic] = $partitions instanceof PartitionsForTopic
                ? $partitions
                : new PartitionsForTopic((string) $topic, array_values($partitions));
        }

        $this->topicPartitions = $packedTopicPartitions;
        $this->version         = $version;
        $this->userData        = $userData;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'version'         => BinarySchema::TYPE_INT16,
            'topicPartitions' => ['topic' => PartitionsForTopic::class],
            'userData'        => BinarySchema::TYPE_BYTEARRAY,
        ];
    }

    /**
     * Returns the assignment as plain partition lists, indexed by the topic they belong to
     *
     * @return array<string, list<int>>
     */
    public function partitions(): array
    {
        $partitions = [];
        foreach ($this->topicPartitions as $topic => $topicAssignment) {
            $partitions[$topic] = $topicAssignment->partitions;
        }

        return $partitions;
    }

    /**
     * Returns the binary representation of this structure, the `member_assignment` of a SyncGroup request
     */
    public function pack(): string
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream($this, $stream);

        return $stream->getBuffer();
    }

    /**
     * Restores the structure from the `member_assignment` bytes of a SyncGroup response
     *
     * @param string $bytes Content of the byte array field, without its length prefix
     */
    public static function unpack(string $bytes): static
    {
        /** @var static $assignment */
        $assignment = BinarySchema::readObjectFromStream(static::class, new StringStream($bytes));

        return $assignment;
    }
}
