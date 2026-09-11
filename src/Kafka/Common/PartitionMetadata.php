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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Information about a topic-partition metadata.
 *
 * <pre>
 *   PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr OfflineReplicas
 *     PartitionErrorCode => int16
 *     PartitionId        => int32
 *     Leader             => int32
 *     Replicas           => [int32]
 *     Isr                => [int32]
 *     OfflineReplicas    => [int32]     -- since version 5
 * </pre>
 *
 * The entry did not change between the versions 0 and 4 - `PARTITION_METADATA_V1` **is** `PARTITION_METADATA_V0` in
 * `MetadataResponse.java` @ 1.1.1 - and {@see PartitionMetadataV0} is the shape those five versions share.
 * **Version 5 of the api (Kafka 1.0, KIP-112/113) appended `OfflineReplicas`**, see {@see self::$offlineReplicas}.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v9)"
 */
class PartitionMetadata implements BinarySchemaInterface
{
    use RestorableTrait;

    /**
     * Version of the Metadata API that this entry is unpacked from
     */
    public const int VERSION = 7;

    /**
     * Epoch of an answer that names none, `MetadataResponse` default of `leader_epoch` @ 2.8.2
     */
    public const int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * The error code for the partition, if any.
     */
    public int $partitionErrorCode = 0;

    /**
     * The id of the partition.
     */
    public int $partitionId = 0;

    /**
     * The id of the broker acting as leader for this partition, `-1` while a leader election is in progress.
     */
    public int $leader = -1;

    /**
     * The set of all nodes that host this partition.
     *
     * @var list<int>
     */
    /**
     * Epoch the leader of this partition is currently on, the field version 7 added (Kafka 2.1, KIP-320)
     *
     * Every leader election of a partition raises it by one, and the record batches the leader appends carry it
     * in their `partition_leader_epoch`. A consumer keeps the newest epoch it has seen per partition, ignores a
     * metadata answer that carries an **older** one - the answer of a broker that has not caught up with the
     * controller yet - and sends it back as the `current_leader_epoch` of its fetches and offset lookups, which
     * is what makes a leader change visible to it instead of being read past.
     *
     * {@see self::UNKNOWN_LEADER_EPOCH} (`-1`) is what an answer below version 7 leaves here: it does not carry
     * the field at all, so "the answer did not say", never "the partition has never had a leader".
     *
     * @since Version 7 of protocol
     */
    public int $leaderEpoch = self::UNKNOWN_LEADER_EPOCH;

    public array $replicas = [];

    /**
     * The set of nodes that are in sync with the leader for this partition.
     *
     * @var list<int>
     */
    public array $isr = [];

    /**
     * The set of replicas of this partition that are offline, the field version 5 added (Kafka 1.0, KIP-112/113).
     *
     * A replica is offline when the broker that holds it is down, or - the reason KIP-112 introduced the field -
     * when the **log directory** that holds it failed on a broker that is otherwise alive: with JBOD a broker keeps
     * more than one `log.dirs` entry, and it survives the loss of one of them by taking the replicas it held
     * offline instead of shutting down. Such a replica is still in `replicas`, is removed from `isr`, and appears
     * here.
     *
     * An answer below version 5 does not carry the field at all and leaves the empty array, which is "the answer
     * did not say", not "every replica is online".
     *
     * @since Version 5 of protocol
     *
     * @var list<int>
     */
    public array $offlineReplicas = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partitionErrorCode' => BinarySchema::TYPE_INT16,
            'partitionId'        => BinarySchema::TYPE_INT32,
            'leader'             => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 7) {
            $scheme['leaderEpoch'] = BinarySchema::TYPE_INT32;
        }
        $scheme['replicas'] = [BinarySchema::TYPE_INT32];
        $scheme['isr']      = [BinarySchema::TYPE_INT32];
        if (static::VERSION >= 5) {
            $scheme['offlineReplicas'] = [BinarySchema::TYPE_INT32];
        }

        return $scheme;
    }
}
