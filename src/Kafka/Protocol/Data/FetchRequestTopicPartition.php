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
 * which is what {@see FetchRequestTopicPartitionV0} lowers the version constant for. The partition entry did not
 * change in any other version.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v7)"
 */
class FetchRequestTopicPartition implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO is packed for
     */
    public const int VERSION = 5;

    /**
     * `LogStartOffset` of a consumer, which is not a follower and therefore has no log of its own
     */
    public const int INVALID_LOG_START_OFFSET = -1;

    /**
     * Id of the partition to fetch from
     */
    public int $partition;

    /**
     * Offset of the first message to fetch
     */
    public int $fetchOffset;

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

    public function __construct(
        int $partition,
        int $fetchOffset,
        int $maxBytes,
        int $logStartOffset = self::INVALID_LOG_START_OFFSET
    ) {
        $this->partition      = $partition;
        $this->fetchOffset    = $fetchOffset;
        $this->maxBytes       = $maxBytes;
        $this->logStartOffset = $logStartOffset;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition'   => BinarySchema::TYPE_INT32,
            'fetchOffset' => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 5) {
            $scheme['logStartOffset'] = BinarySchema::TYPE_INT64;
        }
        $scheme['maxBytes'] = BinarySchema::TYPE_INT32;

        return $scheme;
    }
}
