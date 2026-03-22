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

/**
 * Offsets response DTO
 *
 * Offsets Response (Version: 1) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code timestamp offset
 *       partition => INT32
 *       error_code => INT16
 *       timestamp => INT64
 *       offset => INT64
 */
class OffsetsResponsePartition
{
    /**
     * The partition this response entry corresponds to.
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
     * The timestamp associated with the returned offset
     *
     * @since 0.10.1
     *
     * @var integer
     */
    public $timestamp;

    /**
     * List of offsets in the partition
     *
     * @var integer[]|array
     */
    public $offsets;

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
        [$partition->partition, $partition->errorCode, $partition->timestamp, $offsetsNumber] = array_values($stream->read('Npartition/nerrorCode/Jtimestamp/NoffsetsNumber'));

        $partition->offsets = array_values($stream->read("J{$offsetsNumber}metadata"));

        return $partition;
    }
}
