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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Produce response partition DTO
 *
 * <pre>
 *   Partition ErrorCode Offset
 *     Partition => int32
 *     ErrorCode => int16
 *     Offset    => int64
 * </pre>
 *
 * The `LogAppendTime` of the later protocol lines arrived with version 2 of this API (Kafka 0.10.0).
 *
 * @see docs/protocol/0.8.2.md, section "Produce API (key 0, v0)"
 */
class ProduceResponsePartition implements BinarySchemaInterface
{
    /**
     * The partition this response entry corresponds to.
     */
    public int $partition = 0;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have successfully accepted the produce request.
     */
    public int $errorCode = 0;

    /**
     * The offset assigned to the first message in the message set appended to this partition.
     */
    public int $baseOffset = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'  => BinarySchema::TYPE_INT32,
            'errorCode'  => BinarySchema::TYPE_INT16,
            'baseOffset' => BinarySchema::TYPE_INT64,
        ];
    }
}
