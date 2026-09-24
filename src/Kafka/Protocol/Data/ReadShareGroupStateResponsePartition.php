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
 * The state of one share partition in a ReadShareGroupState answer (key 84, v0, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   PartitionResult => Partition ErrorCode ErrorMessage StateEpoch StartOffset [StateBatches]
 * </pre>
 *
 * `ReadShareGroupStateResponseData.PartitionResult` @ 4.1.0: the error of the partition next to the whole state
 * the share coordinator keeps for it - the state epoch, the share-partition start offset (-1 while it is not
 * initialized) and the delivery state of the records behind it.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupState API (key 84, v0)"
 */
class ReadShareGroupStateResponsePartition implements BinarySchemaInterface
{
    /**
     * Index of the partition
     */
    public int $partition = 0;

    /**
     * Error of the partition, 0 when there is none
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * State epoch of the share partition
     */
    public int $stateEpoch = 0;

    /**
     * Start offset of the share partition, -1 when it is not initialized
     */
    public int $startOffset = 0;

    /**
     * State batches of the share partition
     *
     * @var list<ShareGroupStateBatch>
     */
    public array $stateBatches = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'    => BinarySchema::TYPE_INT32,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
            'stateEpoch'   => BinarySchema::TYPE_INT32,
            'startOffset'  => BinarySchema::TYPE_INT64,
            'stateBatches' => [ShareGroupStateBatch::class],
        ];
    }
}
