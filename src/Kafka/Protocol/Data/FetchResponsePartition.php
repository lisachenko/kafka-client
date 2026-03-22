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

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Common\Record\RecordBatch;

/**
 * Fetch response DTO
 */
class FetchResponsePartition
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
    public $highwaterMarkOffset;

    /**
     * @var array|RecordBatch[]
     */
    public $recordBatch = [];

    /**
     * Unpacks the DTO from the binary buffer
     *
     * @param Stream $stream Binary buffer
     *
     * @return static
     */
    public static function unpack(Stream $stream): static
    {
        $partition = new static();
        [$partition->partition, $partition->errorCode, $partition->highwaterMarkOffset, $batchSize] = array_values($stream->read('Npartition/nerrorCode/JhighwaterMarkOffset/NmessageSetSize'));

        for ($received = 0; $received < $batchSize; $received += ($recordBatch->messageSize + 12)) {
            $recordBatch              = RecordBatch::unpack($stream);
            $partition->recordBatch[] = $recordBatch;
        }

        return $partition;
    }
}
