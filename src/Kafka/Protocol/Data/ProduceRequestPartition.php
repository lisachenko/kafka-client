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

use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Utils\ByteUtils;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Produce request Topic-Partition DTO
 */
class ProduceRequestPartition implements BinarySchemaInterface
{
    /**
     * The partition this request entry corresponds to.
     * @var int
     */
    public $partition;

    /**
     * Data for each separate partition in the topic
     *
     * @var string Should be RecordBatch, but not supported by scheme right now
     *
     * @todo Switch to the RecordBatch binary packet
     */
    public $recordBatch;

    /**
     * @inheritDoc
     */
    public function __construct(int $partition = 0, ?RecordBatch $recordBatch = null)
    {
        $this->partition = $partition;
        $recordBatch ??= new RecordBatch();

        $recordBatchStream = new StringStream();
        BinarySchema::writeObjectToStream($recordBatch, $recordBatchStream);
        $recordBatchBuffer = $recordBatchStream->getBuffer();

        // TODO: Calculation of CRC should be in the RecordBatch, but here we can work with raw buffer in one place
        $prefix = substr($recordBatchBuffer, 0, 17); // firstOffset..magic fields
        $body   = substr($recordBatchBuffer, 21);
        $crc32c = ByteUtils::crc32c($body);

        $recordBatch->crc  = $crc32c;
        $this->recordBatch = $prefix . pack('N', $crc32c) . $body;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'   => BinarySchema::TYPE_INT32,
            'recordBatch' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
