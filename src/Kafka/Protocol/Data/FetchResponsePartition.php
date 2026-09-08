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

use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\IO\Stream;

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
     * Records of the message set of this partition, with the offset that the broker assigned to each of them
     *
     * @var array|Record[]
     */
    public $messageSet = [];

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
        [$partition->partition, $partition->errorCode, $partition->highwaterMarkOffset, $messageSetSize] = array_values($stream->read('Npartition/nerrorCode/JhighwaterMarkOffset/NmessageSetSize'));

        // The message set is a raw byte region: it is read in one go and parsed on its own, because the broker is
        // allowed to cut its last message short and a partial message must never desynchronize the stream
        $buffer = $messageSetSize > 0 ? (string) $stream->read("a{$messageSetSize}data")['data'] : '';

        $partition->messageSet = MessageSet::fromBuffer($buffer)->getRecords();

        return $partition;
    }
}
