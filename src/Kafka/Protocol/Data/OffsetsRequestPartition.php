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
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * One partition of an Offsets (ListOffset) request, version 1
 *
 * <pre>
 *   OffsetsRequestPartition => partition timestamp
 *     partition => INT32
 *     timestamp => INT64
 * </pre>
 *
 * `MaxNumberOfOffsets` only exists in version 0 (`LIST_OFFSET_REQUEST_PARTITION_V0` in `Protocol.java` @ 0.10.2.2):
 * the timestamp-based version 1 of the api, which Kafka 0.10.1 added with KIP-79, answers with exactly one offset
 * per partition and dropped the field. The odd version out therefore lives in {@see OffsetsRequestPartitionV0} and
 * the scheme is selected by {@see OffsetsRequestPartition::VERSION}.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v5), a.k.a. ListOffset"
 */
class OffsetsRequestPartition implements BinarySchemaInterface
{
    /**
     * Version of the Offsets API that this DTO is packed for
     */
    public const int VERSION = 4;

    /**
     * Value of `current_leader_epoch` for a client that does not know the epoch of the partition, or does not care
     *
     * `ListOffsetsRequest.json` @ 2.8.2 gives the field the default -1, and `Partition.checkCurrentLeaderEpoch`
     * @ 2.8.2 skips the fencing check for it - the very same rule the Fetch api follows, see
     * {@see FetchRequestTopicPartition::UNKNOWN_LEADER_EPOCH}.
     */
    public const int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * Id of the partition to list the offsets of
     */
    public int $partition;

    /**
     * Target time in milliseconds, or one of the special values {@see OffsetsRequest::LATEST} /
     * {@see OffsetsRequest::EARLIEST}.
     *
     * In version 1 an ordinary timestamp asks for the offset of the first message whose own timestamp is `>= t`,
     * which the time index of the log resolves. In version 0 the broker knew nothing about the timestamps of the
     * messages and answered with the start offsets of the log segments that were last modified before that time.
     */
    /**
     * Epoch the client believes this partition is being led with, the field version 4 added (Kafka 2.1, KIP-320)
     *
     * An epoch **older** than the one the leader is on is answered **74** `FENCED_LEADER_EPOCH`, a **newer** one
     * **75** `UNKNOWN_LEADER_EPOCH`; both are retriable and both mean "refresh the metadata and ask again".
     * {@see self::UNKNOWN_LEADER_EPOCH} switches the check off, and it is what every version below 4 is served as.
     *
     * @since Version 4 of protocol
     */
    public int $currentLeaderEpoch = self::UNKNOWN_LEADER_EPOCH;

    public int $timestamp;

    /**
     * Maximum number of offsets that the broker may return for this partition
     *
     * @deprecated Since version 1 of the api, which always answers with exactly one offset per partition
     */
    public int $maxNumberOfOffsets;

    public function __construct(
        int $partition,
        int $timestamp,
        int $maxNumberOfOffsets = 1,
        int $currentLeaderEpoch = self::UNKNOWN_LEADER_EPOCH
    ) {
        $this->partition          = $partition;
        $this->timestamp          = $timestamp;
        $this->maxNumberOfOffsets = $maxNumberOfOffsets;
        $this->currentLeaderEpoch = $currentLeaderEpoch;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = ['partition' => BinarySchema::TYPE_INT32];
        // The JSON specification is the wire order, and it puts `CurrentLeaderEpoch` between the partition index
        // and the timestamp
        if (static::VERSION >= 4) {
            $scheme['currentLeaderEpoch'] = BinarySchema::TYPE_INT32;
        }
        $scheme['timestamp'] = BinarySchema::TYPE_INT64;
        if (static::VERSION === 0) {
            $scheme['maxNumberOfOffsets'] = BinarySchema::TYPE_INT32;
        }

        return $scheme;
    }
}
