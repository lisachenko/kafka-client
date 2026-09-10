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

use Protocol\Kafka\Admin\NewPartitions;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic of a CreatePartitions request, i.e. one entry of the `topic_partitions` array
 *
 * <pre>
 *   CreatePartitionsRequestTopic => topic count assignment
 *     topic      => STRING
 *     count      => INT32
 *     assignment => NULLABLE_ARRAY of ARRAY of INT32
 * </pre>
 *
 * `CREATE_PARTITIONS_REQUEST_V0` of `CreatePartitionsRequest.java` @ 1.1.1 declares `count` and `assignment` as a
 * nested `new_partitions` STRUCT of the topic entry; a struct is not counted or delimited on the wire, so the three
 * fields travel exactly as they stand here.
 *
 * `count` is the **total** number of partitions the topic should have afterwards, not the number to add -
 * `NewPartitions.increaseTo(totalCount)` in the Java admin client - and it can only grow: a count below or equal to
 * the current one is answered with the error code 37 (InvalidPartitions).
 *
 * `assignment` is the replica placement of the partitions that are ADDED, one entry per new partition and one broker
 * id per replica of it, and `null` - the default - leaves the placement to the controller. Its length has therefore
 * to be `count` minus the current partition count, and every entry has to have as many brokers as the replication
 * factor of the topic; anything else is the error code 39 (InvalidReplicaAssignment).
 *
 * @see docs/protocol/1.1.md, section "CreatePartitions API (key 37, v0)"
 */
class CreatePartitionsRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic whose partition count should grow
     */
    public string $topic;

    /**
     * Total number of partitions the topic should have afterwards
     */
    public int $count;

    /**
     * Broker ids of every partition that is added, or null to leave the placement to the controller
     *
     * @var list<list<int>>|null
     */
    public ?array $assignment;

    /**
     * @param string                $topic      Name of the topic
     * @param int                   $count      Total number of partitions the topic should have afterwards
     * @param list<list<int>>|null  $assignment Replicas of every added partition, null for the controller's choice
     */
    public function __construct(string $topic, int $count, ?array $assignment = null)
    {
        $this->topic      = $topic;
        $this->count      = $count;
        $this->assignment = $assignment === null
            ? null
            : array_values(array_map(array_values(...), $assignment));
    }

    /**
     * Builds the wire entry of a topic the caller described with a {@see NewPartitions}
     */
    public static function fromNewPartitions(string $topic, NewPartitions $newPartitions): self
    {
        return new self($topic, $newPartitions->totalCount, $newPartitions->assignments);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'count'      => BinarySchema::TYPE_INT32,
            'assignment' => [[BinarySchema::TYPE_INT32], BinarySchema::FLAG_NULLABLE => true],
        ];
    }
}
