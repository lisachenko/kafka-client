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
 * One partition of a InitializeShareGroupState request (Kafka 4.1, KIP-932, v0)
 *
 * `InitializeShareGroupStateRequestData.PartitionData` @ 4.1.0.
 *
 * @see docs/protocol/4.3.md, section "InitializeShareGroupState API (key 83, v0)"
 */
class InitializeShareGroupStateRequestPartition implements BinarySchemaInterface
{
    /**
     * Index of the partition
     */
    public int $partition = 0;

    /**
     * State epoch the share partition is initialized with
     */
    public int $stateEpoch = 0;

    /**
     * Start offset of the share partition, -1 when it is not initialized
     */
    public int $startOffset = 0;

    /**
     * @param int $partition   Index of the partition
     * @param int $stateEpoch  State epoch the share partition is initialized with
     * @param int $startOffset Start offset of the share partition, -1 when it is not initialized
     */
    public function __construct(
        int $partition = 0,
        int $stateEpoch = 0,
        int $startOffset = 0
    ) {
        $this->partition = $partition;
        $this->stateEpoch = $stateEpoch;
        $this->startOffset = $startOffset;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'   => BinarySchema::TYPE_INT32,
            'stateEpoch'  => BinarySchema::TYPE_INT32,
            'startOffset' => BinarySchema::TYPE_INT64,
        ];
    }
}
