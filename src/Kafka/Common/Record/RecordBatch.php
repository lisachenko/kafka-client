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

namespace Protocol\Kafka\Common\Record;

use function microtime;

use Protocol\Kafka\Common\Utils\ByteUtils;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;

/**
 * The record batch structure is common to both the produce and fetch requests.
 *
 * A record batch is just a sequence of records with offset and size information.
 *
 * RecordBatch implementation for magic 2 and above. The schema is given below:
 *
 * RecordBatch =>
 *   BaseOffset => Int64
 *   Length => Int32
 *   PartitionLeaderEpoch => Int32
 *   Magic => Int8
 *   CRC => Uint32
 *   Attributes => Int16
 *   LastOffsetDelta => Int32 // also serves as LastSequenceDelta
 *   FirstTimestamp => Int64
 *   MaxTimestamp => Int64
 *   ProducerId => Int64
 *   ProducerEpoch => Int16
 *   BaseSequence => Int32
 *   Records => [Record]
 *
 * Note that when compression is enabled (see attributes below), the compressed record data is serialized
 * directly following the count of the number of records.
 *
 * The CRC covers the data from the attributes to the end of the batch (i.e. all the bytes that follow the CRC). It is
 * located after the magic byte, which means that clients must parse the magic byte before deciding how to interpret
 * the bytes between the batch length and the magic byte. The partition leader epoch field is not included in the CRC
 * computation to avoid the need to recompute the CRC when this field is assigned for every batch that is received by
 * the broker. The CRC-32C (Castagnoli) polynomial is used for the computation.
 *
 * On Compaction: Unlike the older message formats, magic v2 and above preserves the first and last offset/sequence
 * numbers from the original batch when the log is cleaned. This is required in order to be able to restore the
 * producer's state when the log is reloaded. If we did not retain the last sequence number, then following
 * a partition leader failure, once the new leader has rebuilt the producer state from the log, the next sequence
 * expected number would no longer be in sync with what was written by the client. This would cause an
 * unexpected OutOfOrderSequence error, which is typically fatal. The base sequence number must be preserved for
 * duplicate checking: the broker checks incoming Produce requests for duplicates by verifying that the first and
 * last sequence numbers of the incoming batch match the last from that producer.
 *
 * Note that if all of the records in a batch are removed during compaction, the broker may still retain an empty
 * batch header in order to preserve the producer sequence information as described above. These empty batches
 * are retained only until either a new sequence number is written by the corresponding producer or the producerId
 * is expired from lack of activity.
 *
 * There is no similar need to preserve the timestamp from the original batch after compaction. The FirstTimestamp
 * field therefore always reflects the timestamp of the first record in the batch. If the batch is empty, the
 * FirstTimestamp will be set to -1 (NO_TIMESTAMP).
 *
 * Similarly, the MaxTimestamp field reflects the maximum timestamp of the current records if the timestamp type
 * is CREATE_TIME. For LOG_APPEND_TIME, on the other hand, the MaxTimestamp field reflects the timestamp set
 * by the broker and is preserved after compaction. Additionally, the MaxTimestamp of an empty batch always retains
 * the previous value prior to becoming empty.
 *
 * The current attributes are given below:
 *
 *  -------------------------------------------------------------------------------------------------
 *  | Unused (6-15) | Control (5) | Transactional (4) | Timestamp Type (3) | Compression Type (0-2) |
 *  -------------------------------------------------------------------------------------------------
 *
 * @since 0.11.0
 */
class RecordBatch
{
    /**
     * @see https://github.com/apache/kafka/blob/0.11.0/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java
     */
    public const RECORD_BATCH_OVERHEAD = 49;

    /**
     * Size of the record data
     *
     * @var integer
     */
    public $length;

    /**
     * The new message format has a Magic value of 2
     *
     * @since 0.11.0
     *
     * @var integer
     */
    public $magic = 2;

    /**
     * Control checksum for records
     *
     * @var integer
     */
    public $crc;

    /**
     * The timestamp of the first Record in the batch.
     *
     * The timestamp of each Record in the RecordBatch is its 'TimestampDelta' + 'FirstTimestamp'.
     *
     * @since 0.11.0
     *
     * @var integer
     */
    public $firstTimestamp;

    /**
     * The timestamp of the last Record in the batch.
     *
     * This is used by the broker to ensure the correct behavior even when Records within the batch are compacted out.
     *
     * @since 0.11.0
     *
     * @var integer
     */
    public $maxTimestamp;

    /**
     * Batch of records
     *
     * @var Record[]
     */
    public $records;

    /**
     * @param int $firstOffset
     * @param int $partitionLeaderEpoch
     * @param int $attributes
     * @param int $lastOffsetDelta
     * @param int $producerId
     * @param int $producerEpoch
     * @param int $firstSequence
     */
    public function __construct(
        array $records = [],
        /**
         * Denotes the first offset in the RecordBatch.
         *
         * The 'offsetDelta' of each Record in the batch would be be computed relative to this FirstOffset.
         * In particular, the offset of each Record in the Batch is its 'OffsetDelta' + 'FirstOffset'.
         */
        public $firstOffset = 0,
        /**
         * Introduced with KIP-101, this is set by the broker upon receipt of a produce request and is used to ensure no
         * loss of data when there are leader changes with log truncation. Client developers do not need to worry about
         * setting this value.
         *
         * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation
         *
         * @since 0.11.0
         */
        public $partitionLeaderEpoch = 0,
        /**
         * This byte holds metadata attributes about the message.
         *
         * The lowest 3 bits contain the compression codec used for the message.
         *
         * The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime.
         * The producer should always set this bit to 0. (since 0.10.0)
         *
         * The fifth lowest bit indicates whether the RecordBatch is part of a transaction or not. 0 indicates that the
         * RecordBatch is not transactional, while 1 indicates that it is. (since 0.11.0).
         *
         * The sixth lowest bit indicates whether the RecordBatch includes a control message. 1 indicates that the
         * RecordBatch is contains a control message, 0 indicates that it doesn't. Control messages are used to enable
         * transactions in Kafka and are generated by the broker. Clients should not return control batches (ie. those with
         * this bit set) to applications.
         *
         * @since 0.11.0
         */
        public $attributes = 0,
        /**
         * The offset of the last message in the RecordBatch.
         *
         * This is used by the broker to ensure correct behavior even when Records within a batch are compacted out.
         *
         * @since 0.11.0
         */
        public $lastOffsetDelta = 0,
        $firstTimestamp = null,
        $maxTimestamp = null,
        /**
         * This is the broker assigned producerId received by the 'InitProducerId' request.
         *
         * Clients which want to support idempotent message delivery and transactions must set this field.
         *
         * @since 0.11.0
         * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
         */
        public $producerId = -1,
        /**
         * This is the broker assigned producerEpoch received by the 'InitProducerId' request.
         *
         * Clients which want to support idempotent message delivery and transactions must set this field.
         *
         * @since 0.11.0
         * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
         */
        public $producerEpoch = 0,
        /**
         * This is the producer assigned sequence number which is used by the broker to deduplicate messages.
         *
         * Clients which want to support idempotent message delivery and transactions must set this field.
         * The sequence number for each Record in the RecordBatch is its OffsetDelta + FirstSequence.
         *
         * @since 0.11.0
         * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
         */
        public $firstSequence = 0
    ) {
        $milliSeconds = (int) (microtime(true) * 1e3);

        $this->records              = $records;
        $this->firstTimestamp       = $firstTimestamp ?? $milliSeconds;
        $this->maxTimestamp         = $maxTimestamp ?? $milliSeconds;

        // calculated fields
        $this->length = $this->sizeInBytes(...$records);
    }

    private function sizeInBytes(Record ...$records): int|float
    {
        if (count($records) === 0) {
            return 0;
        }

        $size = self::RECORD_BATCH_OVERHEAD;
        foreach ($records as $record) {
            $size += ByteUtils::sizeOfVarint($record->length) + $record->length;
        }

        return $size;
    }

    /**
     * Unpacks the DTO from the binary buffer
     *
     * @param Stream $stream Binary buffer
     *
     * @return static
     */
    public static function unpack(Stream $stream): static
    {
        $recordBatch = new static();
        [$recordBatch->firstOffset, $recordBatch->length, $recordBatch->partitionLeaderEpoch, $recordBatch->magic, $recordBatch->crc, $recordBatch->attributes, $recordBatch->lastOffsetDelta, $recordBatch->firstTimestamp, $recordBatch->maxTimestamp, $recordBatch->producerId, $recordBatch->producerEpoch, $recordBatch->firstSequence, $recordsNumber] = array_values($stream->read(
            'JfirstOffset/' .
            'Nlength/' .
            'NpartitionLeaderEpoch/' .
            'cmagic/' .
            'Ncrc/' .
            'nattributes/' .
            'NlastOffsetDelta/' .
            'JfirstTimestamp/' .
            'JmaxTimestamp/' .
            'JproducerId/' .
            'nproducerEpoch/' .
            'NfirstSequence/' .
            'NrecordsNumber'
        ));

        for ($index = 0; $index < $recordsNumber; $index++) {
            $recordBatch->records[] = Record::unpack($stream);
        }

        return $recordBatch;
    }

    public function pack(Stream $stream): void
    {
        $payload   = $this->packRecordsBody();
        $this->crc = ByteUtils::crc32c($payload);
        $stream->write(
            'JNNcN',
            $this->firstOffset,
            $this->length,
            $this->partitionLeaderEpoch,
            $this->magic,
            $this->crc
        );
        $stream->writeBuffer($payload);
    }

    /**
     * Packs records into the stream, optionally records could be encoded with specific coded
     *
     * TODO: implement encoders
     *
     * @return string
     */
    private function packRecordsBody()
    {
        $recordStream = new StringStream();

        $recordStream->write(
            'nNJJJnNN',
            $this->attributes,
            $this->lastOffsetDelta,
            $this->firstTimestamp,
            $this->maxTimestamp,
            $this->producerId,
            $this->producerEpoch,
            $this->firstSequence,
            count($this->records)
        );

        foreach ($this->records as $record) {
            $record->pack($recordStream);
        }
        $recordBuffer = $recordStream->getBuffer();
        // @TODO: Encoding of $recordBuffer with codecs

        return $recordBuffer;
    }
}
