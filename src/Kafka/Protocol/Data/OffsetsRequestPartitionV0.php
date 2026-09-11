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
 * One partition of an Offsets (ListOffset) request, version 0
 *
 * <pre>
 *   OffsetsRequestPartition => Partition Time MaxNumberOfOffsets
 *     Partition          => int32
 *     Time               => int64
 *     MaxNumberOfOffsets => int32
 * </pre>
 *
 * Version 0 asks for up to `MaxNumberOfOffsets` segment start offsets, so this class exists solely to raise that
 * field into the scheme that {@see OffsetsRequestPartition::getScheme()} builds.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0, v1 and v2), a.k.a. ListOffset"
 */
final class OffsetsRequestPartitionV0 extends OffsetsRequestPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
