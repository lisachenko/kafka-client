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
 * One partition of a ShareFetch request (key 78, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   FetchPartition => partition_index [acknowledgement_batches]
 *     partition_index         => INT32
 *     acknowledgement_batches => first_offset last_offset [acknowledge_types]
 * </pre>
 *
 * The `FetchPartition` of `ShareFetchRequest.json` @ 4.1.0 at version 1: the `partition_max_bytes` of the early-access
 * version 0 is gone (`"versions": "0"`), and a partition carries the acknowledgements of records it acquired
 * earlier, which a share fetch may piggyback - except the first one of a session, whose acknowledgements the broker
 * refuses with the 42.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 */
final class ShareFetchRequestPartition implements BinarySchemaInterface
{
    /**
     * @param int                             $partitionIndex         Partition to fetch
     * @param list<ShareAcknowledgementBatch> $acknowledgementBatches Acknowledgements of records of this partition
     */
    public function __construct(
        public int $partitionIndex = 0,
        public array $acknowledgementBatches = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex'         => BinarySchema::TYPE_INT32,
            'acknowledgementBatches' => [ShareAcknowledgementBatch::class],
        ];
    }
}
