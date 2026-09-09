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
 * One partition of an Offsets (ListOffset) response v0
 *
 * <pre>
 *   OffsetsResponsePartition => Partition ErrorCode [Offset]
 *     Partition => int32
 *     ErrorCode => int16
 *     Offset    => int64
 * </pre>
 *
 * v0 answers with a list of segment offsets, which is why there is an array here and a single `Timestamp`/`Offset`
 * pair in v1 (Kafka 0.10.1) of the API.
 *
 * @see docs/protocol/0.10.2.md, section "Offsets API (key 2, v0), a.k.a. ListOffset"
 */
class OffsetsResponsePartition implements BinarySchemaInterface
{
    /**
     * The partition this response entry corresponds to.
     */
    public int $partition;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have been answered successfully.
     */
    public int $errorCode;

    /**
     * Offsets of this partition, newest first.
     *
     * For `OffsetsRequest::LATEST` this is the log end offset, for `OffsetsRequest::EARLIEST` the first available
     * offset; for an ordinary timestamp it holds up to `MaxNumberOfOffsets` segment start offsets.
     *
     * @var list<int>
     */
    public array $offsets = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'errorCode' => BinarySchema::TYPE_INT16,
            'offsets'   => [BinarySchema::TYPE_INT64],
        ];
    }
}
