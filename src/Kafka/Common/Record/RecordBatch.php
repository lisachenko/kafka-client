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

use Protocol\Kafka\IO\Stream;

/**
 * The record batch structure is common to both the produce and fetch requests.
 *
 * A record batch is just a sequence of records with offset and size information.
 *
 * @since 0.11.0
 */
class RecordBatch implements \Stringable
{
    /**
     * Offset used in kafka as the log sequence number.
     *
     * When the producer is sending non compressed messages, it can set the offsets to anything. When the producer is
     * sending compressed messages, to avoid server side recompression, each compressed message should have offset
     * starting from 0 and increasing by one for each inner message in the compressed message.
     *
     * @var integer
     */
    public $offset;

    /**
     * Size of the record data
     *
     * @var integer
     */
    public $messageSize;

    /**
     * Record information
     *
     * @var Record
     */
    public $message;

    public static function fromRecord(Record $message, $offset = 0): static
    {
        $recordBatch = new static();

        $recordBatch->offset      = $offset;
        $recordBatch->message     = $message;
        $recordBatch->messageSize = strlen((string) $message);

        return $recordBatch;
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
        [$recordBatch->offset, $recordBatch->messageSize] = array_values($stream->read('Joffset/NmessageSize'));

        $recordBatch->message = Record::unpack($stream);

        return $recordBatch;
    }

    public function __toString(): string
    {
        $message = (string) $this->message;
        $payload = pack(
            "JNa{$this->messageSize}",
            $this->offset,
            $this->messageSize,
            $message
        );

        return $payload;
    }
}
