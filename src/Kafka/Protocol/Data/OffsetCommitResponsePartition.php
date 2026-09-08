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
 * OffsetCommitResponsePartition DTO
 *
 * <pre>
 *   OffsetCommitResponsePartition => partition error_code
 *     partition  => INT32
 *     error_code => INT16
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "OffsetCommit API (key 8, v0, v1 and v2)"
 */
class OffsetCommitResponsePartition implements BinarySchemaInterface
{
    /**
     * The partition this response entry corresponds to.
     */
    public int $partition;

    /**
     * The error from this partition, if any.
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
