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
 * The result of one partition of an AlterShareGroupOffsets request (ApiKey 91, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   AlterShareGroupOffsetsResponsePartition => PartitionIndex ErrorCode ErrorMessage TAG_BUFFER
 *     PartitionIndex => INT32
 *     ErrorCode      => INT16
 *     ErrorMessage   => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "AlterShareGroupOffsets API (key 91, v0)"
 */
final class AlterShareGroupOffsetsResponsePartition implements BinarySchemaInterface
{
    /**
     * Index of the partition
     */
    public int $partitionIndex;

    /**
     * Error of this partition, 0 when its start offset was written
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
