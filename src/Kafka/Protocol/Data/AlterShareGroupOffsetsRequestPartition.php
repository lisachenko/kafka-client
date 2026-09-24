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
 * The new start offset of one partition in an AlterShareGroupOffsets request (ApiKey 91, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   AlterShareGroupOffsetsRequestPartition => PartitionIndex StartOffset TAG_BUFFER
 *     PartitionIndex => INT32
 *     StartOffset    => INT64
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "AlterShareGroupOffsets API (key 91, v0)"
 */
final class AlterShareGroupOffsetsRequestPartition implements BinarySchemaInterface
{
    public function __construct(
        /**
         * Index of the partition
         */
        public int $partitionIndex,
        /**
         * The share-partition start offset the group continues at
         */
        public int $startOffset
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'startOffset'    => BinarySchema::TYPE_INT64,
        ];
    }
}
