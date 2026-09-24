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
 * One partition of a ShareAcknowledge request (key 79, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   AcknowledgePartition => partition_index [acknowledgement_batches]
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1 and v2)"
 */
final class ShareAcknowledgeRequestPartition implements BinarySchemaInterface
{
    /**
     * @param int                             $partitionIndex         Partition of the records
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
