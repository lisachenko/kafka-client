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
 * One partition of a WriteShareGroupState request (Kafka 4.1, KIP-932, v1)
 *
 * <pre>
 *   PartitionData => Partition StateEpoch LeaderEpoch StartOffset DeliveryCompleteCount [StateBatches]
 *     Partition             => INT32
 *     StateEpoch            => INT32
 *     LeaderEpoch           => INT32
 *     StartOffset           => INT64
 *     DeliveryCompleteCount => INT32   -- since version 1 (Kafka 4.2, KIP-1226), "default": -1
 *     StateBatches          => FirstOffset LastOffset DeliveryState DeliveryCount
 * </pre>
 *
 * `WriteShareGroupStateRequestData.PartitionData` @ 4.1.0.
 *
 * **Version 1 (Kafka 4.2, KIP-1226) put `DeliveryCompleteCount` between the start offset and the batches**: "The
 * number of offsets greater than or equal to share-partition start offset for which delivery has been completed"
 * (`WriteShareGroupStateRequest.json` @ 4.2.0, `"ignorable": "true"`, `"default": "-1"`). A partition leader counts
 * the records of its share partition that are acknowledged or archived at or above the start offset
 * (`SharePartition.deliveryCompleteCount` @ 4.3.1) and writes the count with every state, and the share coordinator
 * keeps it with the state it answers a ReadShareGroupStateSummary v1 from - the lag of a share group is the end
 * offset minus the start offset minus this count. **-1** is "not known", the value of a share partition that no
 * consumer has read yet. {@see WriteShareGroupStateRequestPartitionV0} is the entry of the version 0, where the field
 * never reaches the wire.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0 and v1)"
 */
class WriteShareGroupStateRequestPartition implements BinarySchemaInterface
{
    /**
     * Version of the WriteShareGroupState API that this DTO is packed into
     */
    public const int VERSION = 1;

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
     * Number of offsets at or above the start offset whose delivery is complete, -1 when it is not known
     *
     * @since Version 1 of protocol (Kafka 4.2, KIP-1226)
     */
    public int $deliveryCompleteCount = -1;

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
     * @param int                        $deliveryCompleteCount Offsets at or above the start offset whose delivery is
     *        complete, -1 when it is not known (version 1, Kafka 4.2, KIP-1226)
     */
    public function __construct(
        int $partition = 0,
        int $stateEpoch = 0,
        int $leaderEpoch = 0,
        int $startOffset = 0,
        array $stateBatches = [],
        int $deliveryCompleteCount = -1
    ) {
        $this->partition = $partition;
        $this->stateEpoch = $stateEpoch;
        $this->leaderEpoch = $leaderEpoch;
        $this->startOffset = $startOffset;
        $this->stateBatches = array_values($stateBatches);
        $this->deliveryCompleteCount = $deliveryCompleteCount;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition'   => BinarySchema::TYPE_INT32,
            'stateEpoch'  => BinarySchema::TYPE_INT32,
            'leaderEpoch' => BinarySchema::TYPE_INT32,
            'startOffset' => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 1) {
            $scheme['deliveryCompleteCount'] = BinarySchema::TYPE_INT32;
        }
        $scheme['stateBatches'] = [ShareGroupStateBatch::class];

        return $scheme;
    }
}
