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
 * One partition of an OffsetDelete request (key 47, Kafka 2.4, KIP-496)
 *
 * <pre>
 *   OffsetDeleteRequestPartition => PartitionIndex
 *     PartitionIndex => INT32
 * </pre>
 *
 * The whole structure is a single int32: the api throws a committed offset away, so there is nothing to say about
 * a partition beyond naming it. **The api is not flexible** - `OffsetDeleteRequest.json` @ 2.8.2 declares no
 * `flexibleVersions`, which is the "none" of the generator - so this structure carries no tagged-field section
 * although Kafka 2.4 added it.
 *
 * @see docs/protocol/2.8.md, section "OffsetDelete API (key 47, v0)"
 */
class OffsetDeleteRequestPartition implements BinarySchemaInterface
{
    /**
     * Index of the partition whose committed offset is deleted
     */
    public int $partitionIndex;

    public function __construct(int $partitionIndex)
    {
        $this->partitionIndex = $partitionIndex;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex' => BinarySchema::TYPE_INT32,
        ];
    }
}
