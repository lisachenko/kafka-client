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
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;

/**
 * One partition of a DeleteRecords request, i.e. one entry of the `partitions` array of a topic
 *
 * <pre>
 *   DeleteRecordsRequestPartition => partition offset
 *     partition => INT32
 *     offset    => INT64
 * </pre>
 *
 * `DELETE_RECORDS_REQUEST_PARTITION_V0` in `Protocol.java` @ 0.11.0.3: "The offset before which the messages will
 * be deleted." Everything **below** that offset is dropped from the log, and the offset itself becomes the new low
 * watermark (`log_start_offset`) of the partition; {@see DeleteRecordsRequest::HIGH_WATERMARK} (-1) asks for
 * everything up to the high watermark, i.e. for every record that is fully replicated.
 *
 * @see docs/protocol/2.8.md, section "DeleteRecords API (key 21, v0 and v1)"
 */
class DeleteRecordsRequestPartition implements BinarySchemaInterface
{
    /**
     * Id of the partition to delete records from
     */
    public int $partition;

    /**
     * Offset before which every record of the partition is deleted, or {@see DeleteRecordsRequest::HIGH_WATERMARK}
     */
    public int $offset;

    public function __construct(int $partition, int $offset = DeleteRecordsRequest::HIGH_WATERMARK)
    {
        $this->partition = $partition;
        $this->offset    = $offset;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'offset'    => BinarySchema::TYPE_INT64,
        ];
    }
}
