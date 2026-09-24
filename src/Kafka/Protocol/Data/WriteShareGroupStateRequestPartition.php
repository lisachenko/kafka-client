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
 * One partition of a WriteShareGroupState request (Kafka 4.1, KIP-932, v0)
 *
 * `WriteShareGroupStateRequestData.PartitionData` @ 4.1.0.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0)"
 */
class WriteShareGroupStateRequestPartition implements BinarySchemaInterface
{
    /**
     * Index of the partition
     */
    public int $partition = 0;

    /**
     * State epoch of the share partition
     */
    public int $stateEpoch = 0;

    /**
     * Leader epoch of the share partition
     */
    public int $leaderEpoch = 0;

    /**
     * Start offset of the share partition, -1 when it does not change
     */
    public int $startOffset = 0;

    /**
     * State batches of the share partition
     *
     * @var list<ShareGroupStateBatch>
     */
    public array $stateBatches = [];

    /**
     * @param int                        $partition    Index of the partition
     * @param int                        $stateEpoch   State epoch of the share partition
     * @param int                        $leaderEpoch  Leader epoch of the share partition
     * @param int                        $startOffset  Start offset of the share partition, -1 when it does not change
     * @param list<ShareGroupStateBatch> $stateBatches State batches of the share partition
     */
    public function __construct(
        int $partition = 0,
        int $stateEpoch = 0,
        int $leaderEpoch = 0,
        int $startOffset = 0,
        array $stateBatches = []
    ) {
        $this->partition = $partition;
        $this->stateEpoch = $stateEpoch;
        $this->leaderEpoch = $leaderEpoch;
        $this->startOffset = $startOffset;
        $this->stateBatches = array_values($stateBatches);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'    => BinarySchema::TYPE_INT32,
            'stateEpoch'   => BinarySchema::TYPE_INT32,
            'leaderEpoch'  => BinarySchema::TYPE_INT32,
            'startOffset'  => BinarySchema::TYPE_INT64,
            'stateBatches' => [ShareGroupStateBatch::class],
        ];
    }
}
