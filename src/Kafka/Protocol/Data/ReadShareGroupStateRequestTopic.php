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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic of a ReadShareGroupState request, named by its id (Kafka 4.1, KIP-932, v0)
 *
 * `ReadShareGroupStateRequestData.ReadStateData` @ 4.1.0: the topic id and the partitions of the share group
 * whose state is read.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupState API (key 84, v0)"
 */
class ReadShareGroupStateRequestTopic implements BinarySchemaInterface
{
    /**
     * The 16 raw bytes of the topic id
     */
    public string $topicId = '';

    /**
     * Partitions of this topic, indexed by the partition index
     *
     * @var array<int, ReadShareGroupStateRequestPartition>
     */
    public array $partitions = [];

    /**
     * @param string                                    $topicId    The 16 raw bytes of the topic id
     * @param list<ReadShareGroupStateRequestPartition> $partitions Partitions of this topic, indexed by the partition index
     */
    public function __construct(
        string $topicId = '',
        array $partitions = []
    ) {
        $this->topicId = $topicId;
        foreach ($partitions as $item) {
            $this->partitions[$item->partition] = $item;
        }
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => ['partition' => ReadShareGroupStateRequestPartition::class],
        ];
    }
}
