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

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\TaggedField;

/**
 * One partition of a Fetch request
 *
 * <pre>
 *   FetchRequestTopicPartition => Partition FetchOffset LogStartOffset MaxBytes
 *     Partition      => int32
 *     FetchOffset    => int64
 *     LogStartOffset => int64
 *     MaxBytes       => int32
 * </pre>
 *
 * `LogStartOffset` only exists since FetchRequest v5 (Kafka 0.11, KIP-107) and is absent from the versions 0 to 4,
 * which is what {@see FetchRequestTopicPartitionV0} lowers the version constant for; the versions 5 to 8 carry it
 * and nothing else, which is {@see FetchRequestTopicPartitionV5}.
 *
 * **Version 9 (Kafka 2.1, KIP-320) inserted `current_leader_epoch`** between the partition index and the fetch
 * offset, see {@see self::$currentLeaderEpoch}, and **version 12 (Kafka 2.7, KIP-595) `last_fetched_epoch`**
 * between the fetch offset and the log start offset, see {@see self::$lastFetchedEpoch};
 * {@see FetchRequestTopicPartitionV9} keeps the entry of the versions 9 to 11. A version 12 entry also ends in
 * the tagged-field section of a flexible structure, which the schema engine writes on its own.
 *
 * **Version 17 (Kafka 3.9, KIP-853) puts the first tagged field of its own into that section**: the
 * `replica_directory_id` of {@see self::$replicaDirectoryId}, tag 0, which a **follower** writes to name the log
 * directory its replica of this partition lives in. A consumer leaves it at the zero uuid, which is the default of
 * the specification and is therefore not written at all, so a version 17 consumer entry is the version 12 entry
 * byte for byte; {@see FetchRequestTopicPartitionV12} keeps the entry of the versions 12 to 16.
 *
 * **Version 18 (Kafka 4.1, KIP-1166) puts a second tagged field there**: the `high_watermark` of
 * {@see self::$highWatermark}, tag 1, the high watermark a follower knows of the partition. Its default is
 * `Long.MAX_VALUE`, "the feature is not supported", so a consumer entry of version 18 is again the entry of version
 * 17 byte for byte; {@see FetchRequestTopicPartitionV17} keeps that entry. This class is the entry of version 18.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)", "The leader epoch (KIP-320)",
 *      "Epoch validation in the fetch itself (v12, KIP-595)", "The replica directory id of KIP-853 (v17)" and
 *      "The high watermark of a follower, KIP-1166 (v18)"
 */
class FetchRequestTopicPartition implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO is packed for
     */
    public const int VERSION = 18;

    /**
     * Default of the `high_watermark` of version 18: `Long.MAX_VALUE`, "the feature is not supported" (KIP-1166)
     */
    public const int HIGH_WATERMARK_NOT_SUPPORTED = PHP_INT_MAX;

    /**
     * The `high_watermark` of a follower that does not know the high watermark of the partition yet
     */
    public const int UNKNOWN_HIGH_WATERMARK = -1;

    /**
     * `LogStartOffset` of a consumer, which is not a follower and therefore has no log of its own
     */
    public const int INVALID_LOG_START_OFFSET = -1;

    /**
     * Id of the partition to fetch from
     */
    public int $partition;

    /**
     * Value of `current_leader_epoch` for a client that does not know the epoch of the partition, or does not care
     *
     * `FetchRequest.json` @ 2.8.2 gives the field the default -1, and a broker skips the fencing check for it:
     * `Partition.checkCurrentLeaderEpoch` @ 2.8.2 returns `Errors.NONE` for `Optional.empty`, which is what -1
     * decodes to. That is what every call of this client sends unless the caller knows better.
     */
    public const int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * Value of `last_fetched_epoch` of a client that has read no record of the partition yet (KIP-595)
     */
    public const int UNKNOWN_LAST_FETCHED_EPOCH = -1;

    /**
     * Offset of the first message to fetch
     */
    public int $fetchOffset;

    /**
     * Epoch the client believes this partition is being led with, the field version 9 added (Kafka 2.1, KIP-320)
     *
     * It fences a consumer whose metadata is out of date, in both directions
     * (`Partition.checkCurrentLeaderEpoch` @ 2.8.2): an epoch **older** than the one the leader is on is
     * **74** `FENCED_LEADER_EPOCH`, a **newer** one - a client that has seen metadata this broker has not caught
     * up with - is **75** `UNKNOWN_LEADER_EPOCH`. Both are retriable and both mean "refresh the metadata and ask
     * again", never "reset the position".
     *
     * {@see self::UNKNOWN_LEADER_EPOCH} (`-1`) switches the check off and is what a request below version 9 is
     * served as, because it does not carry the field at all.
     *
     * @since Version 9 of protocol
     */
    public int $currentLeaderEpoch = self::UNKNOWN_LEADER_EPOCH;

    /**
     * Maximum number of bytes of the message set that the broker may put into the response for this partition.
     *
     * This is `max.partition.fetch.bytes`, and it keeps that per-partition meaning next to the request-level
     * `MaxBytes` that version 3 of the api added ({@see \Protocol\Kafka\Protocol\Request\FetchRequest::$maxBytes}).
     * Up to version 2 the broker cuts the message set of a partition off at this limit without caring about message
     * boundaries, so a message that is bigger comes back as an incomplete set and the partition makes no progress;
     * from version 3 on the first non-empty partition of an answer ignores the limit and returns at least one
     * complete message.
     */
    public int $maxBytes;

    /**
     * Earliest offset the *sender* still holds, the field version 5 added (KIP-107).
     *
     * It is meant for a **follower** replica, which tells the leader where its own log begins so that the leader
     * can keep the log start offsets of the partition in step; an ordinary consumer has no log and sends
     * {@see self::INVALID_LOG_START_OFFSET}, exactly as `FetchRequest.PartitionData` of the Java consumer does.
     *
     * @since Version 5 of protocol
     */
    public int $logStartOffset = self::INVALID_LOG_START_OFFSET;

    /**
     * Epoch of the **last record this client really read** from the partition, the field of version 12.
     *
     * **KIP-595** (Kafka 2.7) turned the truncation detection of KIP-320 around: instead of asking the leader
     * where an epoch ended with an OffsetForLeaderEpoch, a fetcher states the epoch of the record it stopped at
     * and the leader compares it with its own log. When they diverge, the answer carries the
     * `diverging_epoch` of {@see FetchResponsePartition::$divergingEpoch} - the largest epoch and its end offset
     * from which the two logs are known to differ - and the fetcher truncates to that offset without a second
     * request. The field is used by the raft replication of KIP-595 and by a follower; an ordinary consumer of
     * this client sends {@see self::UNKNOWN_LAST_FETCHED_EPOCH}, which is what the Java consumer sends as well
     * while it validates its positions the KIP-320 way.
     *
     * @since Version 12 of protocol
     */
    public int $lastFetchedEpoch = self::UNKNOWN_LAST_FETCHED_EPOCH;

    /**
     * Directory the **follower** that sends this fetch keeps its replica of the partition in (KIP-853)
     *
     * The tagged field (tag 0) that version 17 added, `"ignorable": true` in `FetchRequest.json` @ 3.9.2: the 16
     * raw bytes of the uuid a broker wrote into the `meta.properties` of one of its log directories. KRaft records
     * which directory holds which replica (`AssignmentsManager`, the `DirectoryId` of the `PartitionRecord`), and
     * a follower that has moved a replica between its own disks says so in the fetch itself instead of in a
     * separate api call.
     *
     * A **consumer has nothing to say here**, and neither has a follower that does not track its directories:
     * {@see Uuid::ZERO} is the default of a `uuid` field of the specification, and a tagged field whose value is
     * its default is left off the wire altogether, so a version 17 frame of this client is the version 16 frame
     * byte for byte.
     *
     * @since Version 17 of protocol (Kafka 3.9, KIP-853)
     */
    public string $replicaDirectoryId = Uuid::ZERO;

    /**
     * High watermark of the partition that the **follower** sending this fetch knows (KIP-1166)
     *
     * The tagged field (tag 1) that version 18 added, `"ignorable": true` in `FetchRequest.json` @ 4.1.0: "The
     * high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the
     * feature is not supported." The leader of a KRaft quorum compares it with its own high watermark and answers a
     * follower that is behind at once instead of parking the fetch (`KafkaRaftClient.isHighWatermarkUpdated` @
     * 4.1.0). A **consumer has nothing to say here**: {@see self::HIGH_WATERMARK_NOT_SUPPORTED} is the default of
     * the specification and a tagged field whose value is its default is left off the wire.
     *
     * @since Version 18 of protocol (Kafka 4.1, KIP-1166)
     */
    public int $highWatermark = self::HIGH_WATERMARK_NOT_SUPPORTED;

    public function __construct(
        int $partition,
        int $fetchOffset,
        int $maxBytes,
        int $logStartOffset = self::INVALID_LOG_START_OFFSET,
        int $currentLeaderEpoch = self::UNKNOWN_LEADER_EPOCH,
        int $lastFetchedEpoch = self::UNKNOWN_LAST_FETCHED_EPOCH,
        string $replicaDirectoryId = Uuid::ZERO,
        int $highWatermark = self::HIGH_WATERMARK_NOT_SUPPORTED
    ) {
        $this->partition          = $partition;
        $this->fetchOffset        = $fetchOffset;
        $this->maxBytes           = $maxBytes;
        $this->logStartOffset     = $logStartOffset;
        $this->currentLeaderEpoch = $currentLeaderEpoch;
        $this->lastFetchedEpoch   = $lastFetchedEpoch;
        $this->replicaDirectoryId = $replicaDirectoryId;
        $this->highWatermark      = $highWatermark;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = ['partition' => BinarySchema::TYPE_INT32];
        // The JSON specification is the wire order, and it puts `CurrentLeaderEpoch` BETWEEN the partition index
        // and the fetch offset - not behind the offset, as the KIP reads
        if (static::VERSION >= 9) {
            $scheme['currentLeaderEpoch'] = BinarySchema::TYPE_INT32;
        }
        $scheme['fetchOffset'] = BinarySchema::TYPE_INT64;
        // The epoch of the last record this client really read, which version 12 (Kafka 2.7, KIP-595) put
        // between the fetch offset and the log start offset - the field order of `FetchRequest.json` @ 2.8.2
        if (static::VERSION >= 12) {
            $scheme['lastFetchedEpoch'] = BinarySchema::TYPE_INT32;
        }
        if (static::VERSION >= 5) {
            $scheme['logStartOffset'] = BinarySchema::TYPE_INT64;
        }
        $scheme['maxBytes'] = BinarySchema::TYPE_INT32;
        // The `replica_directory_id` of version 17 (KIP-853) is a TAGGED field (tag 0) and therefore travels in
        // the section at the end of the entry, and only when it is not the zero uuid of the specification
        if (static::VERSION >= 17) {
            $scheme['replicaDirectoryId'] = new TaggedField(0, BinarySchema::TYPE_UUID, Uuid::ZERO);
        }
        // The `high_watermark` of version 18 (KIP-1166) is the tag 1 of the same section, written only when it is
        // not the Long.MAX_VALUE of the specification
        if (static::VERSION >= 18) {
            $scheme['highWatermark'] = new TaggedField(1, BinarySchema::TYPE_INT64, self::HIGH_WATERMARK_NOT_SUPPORTED);
        }

        return $scheme;
    }
}
