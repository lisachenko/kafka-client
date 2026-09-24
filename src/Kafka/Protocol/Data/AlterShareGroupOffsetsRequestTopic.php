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
 * One topic of an AlterShareGroupOffsets request (ApiKey 91, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   AlterShareGroupOffsetsRequestTopic => TopicName [Partitions] TAG_BUFFER
 *     TopicName  => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of {@see AlterShareGroupOffsetsRequestPartition}
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "AlterShareGroupOffsets API (key 91, v0)"
 */
final class AlterShareGroupOffsetsRequestTopic implements BinarySchemaInterface
{
    /**
     * New start offset of every partition, indexed by the partition index
     *
     * @var array<int, AlterShareGroupOffsetsRequestPartition>
     */
    public array $partitions;

    /**
     * @param string          $topicName    Name of the topic
     * @param array<int, int> $startOffsets New start offset of every partition, as partition => offset
     */
    public function __construct(
        /**
         * Name of the topic
         */
        public string $topicName,
        array $startOffsets
    ) {
        $packed = [];
        foreach ($startOffsets as $partition => $startOffset) {
            $packed[(int) $partition] = new AlterShareGroupOffsetsRequestPartition((int) $partition, $startOffset);
        }
        $this->partitions = $packed;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicName'  => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => AlterShareGroupOffsetsRequestPartition::class],
        ];
    }
}
