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
 * One partition of an Offsets (ListOffset) response, version 1
 *
 * <pre>
 *   OffsetsResponsePartition => partition error_code timestamp offset
 *     partition  => INT32
 *     error_code => INT16
 *     timestamp  => INT64
 *     offset     => INT64
 * </pre>
 *
 * Version 1 answers with **one** offset and the timestamp of the message it belongs to; version 0 answered with a
 * list of segment start offsets and had no timestamp at all, which is what {@see OffsetsResponsePartitionV0} reads
 * into {@see self::$offsets}. The scheme is selected by {@see OffsetsResponsePartition::VERSION}.
 *
 * Both `timestamp` and `offset` are {@see self::UNKNOWN_TIMESTAMP} / {@see self::UNKNOWN_OFFSET} when the broker
 * found nothing: a failed partition, and a target timestamp that is above the timestamp of every message of the
 * log. A request for {@see OffsetsRequest::LATEST} or {@see OffsetsRequest::EARLIEST} succeeds with a real offset
 * and the timestamp -1, because the broker does not read the message the offset points at.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v5), a.k.a. ListOffset"
 */
class OffsetsResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the Offsets API that this DTO is unpacked from
     */
    public const int VERSION = 4;

    /**
     * Epoch of an answer that names none, `ListOffsetsResponse` default of `leader_epoch` @ 2.8.2
     */
    public const int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * The timestamp of an answer that carries none, `ListOffsetResponse.UNKNOWN_TIMESTAMP` @ 0.10.2.2
     */
    public const int UNKNOWN_TIMESTAMP = -1;

    /**
     * The offset of an answer that found none, `ListOffsetResponse.UNKNOWN_OFFSET` @ 0.10.2.2
     */
    public const int UNKNOWN_OFFSET = -1;

    /**
     * The partition this response entry corresponds to.
     */
    public int $partition;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have been answered successfully.
     */
    public int $errorCode;

    /**
     * The timestamp of the message the returned offset points at, or {@see self::UNKNOWN_TIMESTAMP}.
     *
     * @since Version 1 of protocol
     */
    public int $timestamp = self::UNKNOWN_TIMESTAMP;

    /**
     * The offset that was found, or {@see self::UNKNOWN_OFFSET} when there is none.
     *
     * @since Version 1 of protocol
     */
    public int $offset = self::UNKNOWN_OFFSET;

    /**
     * Offsets of this partition, newest first.
     *
     * @deprecated Since version 1 of the api, which answers with the single {@see self::$offset}
     *
     * @var list<int>
     */
    /**
     * Epoch the leader was on when it read the answered offset, the field version 4 added (Kafka 2.1, KIP-320)
     *
     * A consumer that seeks to the answered `offset` stores this epoch next to it and sends it back as the
     * `current_leader_epoch` of its next fetch, so that a leader change between the lookup and the fetch is
     * noticed instead of read past. It is {@see self::UNKNOWN_LEADER_EPOCH} in every answer below version 4,
     * which does not carry the field at all, and for a lookup that found nothing.
     *
     * @since Version 4 of protocol
     */
    public int $leaderEpoch = self::UNKNOWN_LEADER_EPOCH;

    public array $offsets = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition' => BinarySchema::TYPE_INT32,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
        if (static::VERSION >= 1) {
            $scheme['timestamp'] = BinarySchema::TYPE_INT64;
            $scheme['offset']    = BinarySchema::TYPE_INT64;
        } else {
            $scheme['offsets'] = [BinarySchema::TYPE_INT64];
        }
        if (static::VERSION >= 4) {
            $scheme['leaderEpoch'] = BinarySchema::TYPE_INT32;
        }

        return $scheme;
    }
}
