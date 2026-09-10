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
 * The result of deleting the records of one partition, i.e. one entry of the `partitions` array of a topic
 *
 * <pre>
 *   DeleteRecordsResponsePartition => partition low_watermark error_code
 *     partition     => INT32
 *     low_watermark => INT64
 *     error_code    => INT16
 * </pre>
 *
 * `DELETE_RECORDS_RESPONSE_PARTITION_V0` in `Protocol.java` @ 0.11.0.3: "Smallest available offset of all live
 * replicas". The field is the new `logStartOffset` of the partition - the offset of the first record that is still
 * readable - and it is exactly the same number that a Fetch v5 answer reports as `log_start_offset` and that an
 * Offsets request with {@see \Protocol\Kafka\Protocol\Request\OffsetsRequest::EARLIEST} returns.
 *
 * A partition that failed carries {@see self::INVALID_LOW_WATERMARK} (-1) instead of a watermark, because
 * `DeleteRecordsRequest.getErrorResponse` and every error branch of `ReplicaManager.deleteRecords` build the entry
 * with that constant.
 *
 * @see docs/protocol/1.1.md, section "DeleteRecords API (key 21, v0)"
 */
class DeleteRecordsResponsePartition implements BinarySchemaInterface
{
    /**
     * Watermark of a partition that was not deleted at all, `DeleteRecordsResponse.INVALID_LOW_WATERMARK`
     */
    public const int INVALID_LOW_WATERMARK = -1;

    /**
     * Id of the partition this entry belongs to
     */
    public int $partition;

    /**
     * New low watermark of the partition, i.e. the offset of its first readable record
     */
    public int $lowWatermark;

    /**
     * Error code of this partition, 0 when the records were deleted
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'    => BinarySchema::TYPE_INT32,
            'lowWatermark' => BinarySchema::TYPE_INT64,
            'errorCode'    => BinarySchema::TYPE_INT16,
        ];
    }
}
