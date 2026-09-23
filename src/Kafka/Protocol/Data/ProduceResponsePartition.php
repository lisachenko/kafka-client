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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\TaggedField;

/**
 * Produce response partition DTO
 *
 * <pre>
 *   Partition ErrorCode Offset LogAppendTime LogStartOffset
 *     Partition      => int32
 *     ErrorCode      => int16
 *     Offset         => int64
 *     LogAppendTime  => int64
 *     LogStartOffset => int64
 * </pre>
 *
 * `LogAppendTime` arrived with version 2 of this API (Kafka 0.10.0, message format v1) and is absent from the
 * answer of a version 0 or 1 request, which is what {@see ProduceResponsePartitionV0} decodes. The versions 3 and
 * 4 of the api left the partition entry untouched - `PRODUCE_RESPONSE_V4` **is** `PRODUCE_RESPONSE_V3` **is**
 * `PRODUCE_RESPONSE_V2` in `ProduceResponse.schemaVersions()` @ 1.1.1, verified against the broker - and
 * {@see ProduceResponsePartitionV2} is the entry those three versions share.
 *
 * **Version 5 (Kafka 1.0) appended `LogStartOffset`**, which is what this class adds, see
 * {@see self::$logStartOffset}; **version 8** (Kafka 2.4, KIP-467) the `record_errors` array and the
 * `error_message` of a refused batch, see {@see self::$recordErrors}, and **version 10** (Kafka 3.7, KIP-951)
 * the **tagged** `current_leader`, see {@see self::$currentLeader}, which is the only difference between this
 * entry and {@see ProduceResponsePartitionV8}.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v11)" and "The leader discovery of KIP-951 (v10)"
 */
class ProduceResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO is unpacked from
     */
    public const int VERSION = 10;

    /**
     * Value of `LogStartOffset` for an answer of a version below 5, which does not carry the field
     */
    public const int INVALID_OFFSET = -1;

    /**
     * Value of `LogAppendTime` for a topic that stamps its records with a `CreateTime`, i.e. "no append time"
     */
    public const int NO_LOG_APPEND_TIME = -1;

    /**
     * The partition this response entry corresponds to.
     */
    public int $partition = 0;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have successfully accepted the produce request.
     */
    public int $errorCode = 0;

    /**
     * The offset assigned to the first message in the message set appended to this partition.
     */
    public int $baseOffset = 0;

    /**
     * Time the broker assigned to every message of the appended set, or -1 when it kept the producer's timestamps.
     *
     * With `message.timestamp.type=LogAppendTime` on the topic the broker overwrites the timestamp of every message
     * it appends with its own clock and reports that value here, once for the whole set - every message of the set
     * carries it. With the default `CreateTime` the field is `-1` ({@see self::NO_LOG_APPEND_TIME}) and the producer
     * may assume that the timestamps it sent have been stored as they were.
     *
     * Unit is milliseconds since the beginning of the epoch (midnight Jan 1, 1970 UTC).
     *
     * @since Version 2 of protocol
     *
     * @see \Protocol\Kafka\Producer\RecordMetadata::$timestamp
     */
    public int $logAppendTime = self::NO_LOG_APPEND_TIME;

    /**
     * Earliest offset the log of this partition still holds, the field version 5 added (Kafka 1.0).
     *
     * Everything below it has been deleted by the retention of the topic or by a DeleteRecords request, so a
     * producer that is answered with 59 `UnknownProducerIdException` can tell a **spurious** out-of-order sequence
     * - the broker forgot the producer state of this partition because its records fell below this offset, and the
     * producer only has to reset its sequence numbers - from a real gap in its own sequence, which is a bug of the
     * producer. `ProduceResponse.INVALID_OFFSET` @ 1.1.1 is -1, which is what an answer below version 5 leaves
     * here, see {@see self::INVALID_OFFSET}.
     *
     * @since Version 5 of protocol
     *
     * @see \Protocol\Kafka\Producer\Internals\TransactionManager
     */
    public int $logStartOffset = self::INVALID_OFFSET;

    /**
     * Milliseconds the broker delayed the answer this partition arrived in, because of a produce quota.
     *
     * This is **not** a field of the wire format - the Produce API reports its `ThrottleTime` once per response,
     * behind the topics array - and it is therefore not part of {@see self::getScheme()}. The client copies the
     * value of an answer onto every partition of it, because a batch is split by partition leaders and each of
     * those answers carries a throttle time of its own.
     *
     * @see \Protocol\Kafka\Producer\RecordMetadata::$throttleTimeMs
     * @see docs/protocol/4.3.md, section "Quotas and throttle time"
     */
    public int $throttleTimeMs = 0;

    /**
     * Records of the sent batch that the broker refused, indexed by their position in the batch (KIP-467).
     *
     * Empty for a partition that was appended, and empty as well for every answer below version **8**, which
     * does not carry the field at all: a batch that fails validation is then a bare error code - **87**
     * `INVALID_RECORD` most of the time - and the producer cannot tell which of its records caused it. The array
     * is what version 8 (Kafka 2.4) added, see {@see ProduceResponseRecordError}.
     *
     * @var array<int, ProduceResponseRecordError>
     *
     * @since Version 8 of protocol
     */
    public array $recordErrors = [];

    /**
     * The broker's own summary of why the batch was refused, `null` when it sent none.
     *
     * The common root cause of the records of {@see self::$recordErrors}: a batch that was dropped for one
     * reason carries the reason here once, and the entries of the array name the records it applies to. Every
     * answer below version 8 leaves it `null`.
     *
     * @since Version 8 of protocol
     */
    public ?string $errorMessage = null;

    /**
     * The node and the epoch this partition is really led with, `null` when the broker did not say (KIP-951).
     *
     * The **tagged** field (tag 0) that version 10 (Kafka 3.7) added, and the only thing that version added to a
     * partition entry. A broker fills it in for a partition it refused with **6** `NotLeaderForPartition` and
     * whose new leader it knows, and names the address of that node in the top-level
     * {@see \Protocol\Kafka\Protocol\Request\ProduceResponse::$nodeEndpoints} of the same answer; every other
     * entry leaves it off the wire, which is what a tagged field whose value is its default does.
     *
     * An answer below version 10 has no room for it at all, and a producer that reads `null` here is exactly
     * where it was before the KIP: it has to refresh its metadata to learn the new leader.
     *
     * @since Version 10 of protocol (Kafka 3.7, KIP-951)
     */
    public ?ProduceResponseCurrentLeader $currentLeader = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition'  => BinarySchema::TYPE_INT32,
            'errorCode'  => BinarySchema::TYPE_INT16,
            'baseOffset' => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 2) {
            $scheme['logAppendTime'] = BinarySchema::TYPE_INT64;
        }
        if (static::VERSION >= 5) {
            $scheme['logStartOffset'] = BinarySchema::TYPE_INT64;
        }
        if (static::VERSION >= 8) {
            $scheme['recordErrors']  = ['batchIndex' => ProduceResponseRecordError::class];
            $scheme['errorMessage']  = BinarySchema::TYPE_NULLABLE_STRING;
        }
        // The `current_leader` of version 10 is a TAGGED field (tag 0) and is therefore written at the end of the
        // entry, and only for a partition whose leader the broker really named
        if (static::VERSION >= 10) {
            $scheme['currentLeader'] = new TaggedField(0, ProduceResponseCurrentLeader::class, null);
        }

        return $scheme;
    }
}
