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

/**
 * One partition of an Offsets (ListOffset) response, version 0
 *
 * <pre>
 *   OffsetsResponsePartition => Partition ErrorCode [Offset]
 *     Partition => int32
 *     ErrorCode => int16
 *     Offset    => int64
 * </pre>
 *
 * Version 0 answers with a list of segment start offsets, newest first - the log end offset for
 * `OffsetsRequest::LATEST`, the first offset that is still on disk for `OffsetsRequest::EARLIEST`, and up to
 * `MaxNumberOfOffsets` of them for an ordinary time. The class exists only to lower the version constant that
 * {@see OffsetsResponsePartition::getScheme()} follows, so the answer arrives in
 * {@see OffsetsResponsePartition::$offsets} while `timestamp` and `offset` keep their unknown values.
 *
 * @see docs/protocol/0.10.2.md, section "Offsets API (key 2, v0 and v1), a.k.a. ListOffset"
 */
final class OffsetsResponsePartitionV0 extends OffsetsResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
