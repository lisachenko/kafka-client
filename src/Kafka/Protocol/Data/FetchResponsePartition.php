<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Common\Record\RecordBatch;

/**
 * Fetch response topic partition header
 *
 * partition_header => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions]
 *   partition => INT32
 *   error_code => INT16
 *   high_watermark => INT64
 *   last_stable_offset => INT64
 *   log_start_offset => INT64
 *   aborted_transactions => producer_id first_offset
 *     producer_id => INT64
 *     first_offset => INT64
 */
class FetchResponsePartition implements BinarySchemaInterface
{
    /**
     * The id of the partition this response is for.
     *
     * @var integer
     */
    public $partition;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have successfully accepted the produce request.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * The offset at the end of the log for this partition. This can be used by the client to determine how many
     * messages behind the end of the log they are.
     *
     * @var integer
     */
    public $highWaterMarkOffset;

    /**
     * The last stable offset (or LSO) of the partition.
     *
     * This is the last offset such that the state of all transactional records prior to this offset have been decided
     * (ABORTED or COMMITTED)
     *
     * @since version 4
     *
     * @var integer
     */
    public $lastStableOffset;

    /**
     * Earliest available offset.
     *
     * @since version 5
     *
     * @var integer
     */
    public $logStartOffset;

    /**
     * List of aborted transactions
     *
     * @since version 4
     *
     * @var FetchResponseAbortedTransaction[]
     */
    public $abortedTransactions = [];

    /**
     * @var string
     */
    public $recordBatchBuffer;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'           => BinarySchema::TYPE_INT32,
            'errorCode'           => BinarySchema::TYPE_INT16,
            'highWaterMarkOffset' => BinarySchema::TYPE_INT64,
            'lastStableOffset'    => BinarySchema::TYPE_INT64,
            'logStartOffset'      => BinarySchema::TYPE_INT64,
            'abortedTransactions' => ['producerId' => FetchResponseAbortedTransaction::class],
            // TODO: this should be actualy dynamic array of RecordBatch::class entities
            'recordBatchBuffer'   => BinarySchema::TYPE_BYTEARRAY,
        ];
    }

    /**
     * Returns collection of RecordBatches
     *
     * TODO: is this possible somehow to do this on BinarySchema level?
     * @return RecordBatch[]
     */
    public function getRecordBatches(): array
    {
        $recordBatches = [];
        // TODO: Avoid creation of temporary string buffer, this should be implemented in reader directly
        $buffer = new StringStream($this->recordBatchBuffer);
        while (!$buffer->isEmpty()) {
            $recordBatches[] = BinarySchema::readObjectFromStream(RecordBatch::class, $buffer);
        }

        return $recordBatches;
    }
}
