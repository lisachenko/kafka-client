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

use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One partition of a ShareFetch answer (key 78, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   PartitionData => partition_index error_code error_message acknowledge_error_code acknowledge_error_message
 *                    current_leader records [acquired_records]
 *     current_leader   => leader_id leader_epoch
 *     records          => COMPACT_RECORDS                  -- not nullable at version 1
 *     acquired_records => first_offset last_offset delivery_count
 * </pre>
 *
 * The `PartitionData` of `ShareFetchResponse.json` @ 4.1.0. A partition answers **two** outcomes: the one of the fetch
 * (`error_code`) and the one of the acknowledgements the request piggybacked for it (`acknowledge_error_code`, the
 * **121** `InvalidRecordState` of a record that is not acquired by this member any more). `records` holds whole
 * record batches of the log - the broker never cuts a batch - and {@see self::$acquiredRecords} says which offsets of
 * them this member acquired; {@see self::acquiredRecords()} is that intersection.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1 and v2)"
 */
final class ShareFetchResponsePartition implements BinarySchemaInterface
{
    /**
     * Partition index
     */
    public int $partitionIndex = 0;

    /**
     * Error code of the fetch of this partition
     */
    public int $errorCode = 0;

    /**
     * Error message of the fetch, null without an error
     */
    public ?string $errorMessage = null;

    /**
     * Error code of the acknowledgements the request carried for this partition
     */
    public int $acknowledgeErrorCode = 0;

    /**
     * Error message of the acknowledgements, null without an error
     */
    public ?string $acknowledgeErrorMessage = null;

    /**
     * Current leader of the partition, filled in only with the error codes 6 and 74 (`0 0` otherwise)
     */
    public ShareLeaderIdAndEpoch $currentLeader;

    /**
     * Record batches of the log, as the raw bytes of the message format v2
     */
    public ?string $records = null;

    /**
     * Ranges of offsets this member acquired
     *
     * @var list<ShareAcquiredRecords>
     */
    public array $acquiredRecords = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex'          => BinarySchema::TYPE_INT32,
            'errorCode'               => BinarySchema::TYPE_INT16,
            'errorMessage'            => BinarySchema::TYPE_NULLABLE_STRING,
            'acknowledgeErrorCode'    => BinarySchema::TYPE_INT16,
            'acknowledgeErrorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
            'currentLeader'           => ShareLeaderIdAndEpoch::class,
            'records'                 => BinarySchema::TYPE_BYTEARRAY,
            'acquiredRecords'         => [ShareAcquiredRecords::class],
        ];
    }

    /**
     * Returns every record of the answer, acquired or not, in offset order
     *
     * @return list<Record>
     */
    public function records(): array
    {
        return array_values(MemoryRecords::fromBuffer($this->records ?? '')->getRecords());
    }

    /**
     * Returns the records this member acquired, each with the delivery count of its range, keyed by offset
     *
     * @return array<int, array{record: Record, deliveryCount: int}>
     */
    public function acquiredRecords(): array
    {
        $acquired = [];
        foreach ($this->records() as $record) {
            foreach ($this->acquiredRecords as $range) {
                if ($record->offset !== null && $record->offset >= $range->firstOffset && $record->offset <= $range->lastOffset) {
                    $acquired[$record->offset] = ['record' => $record, 'deliveryCount' => $range->deliveryCount];
                    break;
                }
            }
        }

        return $acquired;
    }
}
