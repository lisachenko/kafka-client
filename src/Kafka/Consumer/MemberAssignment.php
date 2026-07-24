<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * Consumer Groups: The format of the MemberAssignment field for consumer groups
 *
 * MemberAssignment => Version PartitionAssignment
 *   Version => int16
 *   PartitionAssignment => [Topic [Partition]]
 *     Topic => string
 *     Partition => int32
 *   UserData => bytes
 */
class MemberAssignment implements BinarySchemaInterface
{
    /**
     * This is a version id.
     * @var int
     */
    public $version;

    /**
     * This property holds assignments of topic partitions for member.
     *
     * @var PartitionsForTopic[]
     */
    public $topicPartitions = [];

    /**
     * The UserData field can be used by custom partition assignment strategies.
     *
     * For example, in a sticky partitioning implementation, this field can contain the assignment from the previous
     * generation. In a resource-based assignment strategy, it could include the number of cpus on the machine hosting
     * each consumer instance.
     * @var string
     */
    public $userData;

    /**
     * MemberAssignment constructor.
     *
     * @param array|int[][] $topicPartitions Partition assignments per topic
     * @param int           $version         Optional version
     * @param string        $userData        Additional user data
     */
    public function __construct(array $topicPartitions = [], int $version = 0, string $userData = '')
    {
        $packedTopicAssignment = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopicAssignment[$topic] = new PartitionsForTopic($topic, $partitions);
        }

        $this->topicPartitions = $packedTopicAssignment;
        $this->version         = $version;
        $this->userData        = $userData;
    }

    /**
     * Returns definition of binary packet for the class or object
     *
     * @return array
     */
    public static function getScheme(): array
    {
        return [
            'version'         => BinarySchema::TYPE_INT16,
            'topicPartitions' => ['topic' => PartitionsForTopic::class],
            'userData'        => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
