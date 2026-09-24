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
 * One partition of a ReadShareGroupState request (Kafka 4.1, KIP-932, v0)
 *
 * `ReadShareGroupStateRequestData.PartitionData` @ 4.1.0.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupState API (key 84, v0)"
 */
class ReadShareGroupStateRequestPartition implements BinarySchemaInterface
{
    /**
     * Index of the partition
     */
    public int $partition = 0;

    /**
     * Leader epoch of the share partition, -1 to leave the recorded one alone
     */
    public int $leaderEpoch = 0;

    /**
     * @param int $partition   Index of the partition
     * @param int $leaderEpoch Leader epoch of the share partition, -1 to leave the recorded one alone
     */
    public function __construct(
        int $partition = 0,
        int $leaderEpoch = 0
    ) {
        $this->partition = $partition;
        $this->leaderEpoch = $leaderEpoch;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'   => BinarySchema::TYPE_INT32,
            'leaderEpoch' => BinarySchema::TYPE_INT32,
        ];
    }
}
