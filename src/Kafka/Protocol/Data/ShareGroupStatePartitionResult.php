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
 * The result of one partition of an Initialize-, Write- or DeleteShareGroupState answer (keys 83, 85, 86, v0)
 *
 * <pre>
 *   PartitionResult => Partition ErrorCode ErrorMessage
 *     Partition    => INT32
 *     ErrorCode    => INT16
 *     ErrorMessage => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * `PartitionResult` of `InitializeShareGroupStateResponseData`, `WriteShareGroupStateResponseData` and
 * `DeleteShareGroupStateResponseData` @ 4.1.0: three generated classes with the same three fields, carried here
 * as one. The answers of the two read apis carry the state next to them.
 *
 * @see docs/protocol/4.3.md, section "The share-group state apis (keys 83 to 87) — wire only"
 */
class ShareGroupStatePartitionResult implements BinarySchemaInterface
{
    /**
     * Index of the partition
     */
    public int $partition = 0;

    /**
     * Error of the partition, 0 when there is none
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
            'partition'    => BinarySchema::TYPE_INT32,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
