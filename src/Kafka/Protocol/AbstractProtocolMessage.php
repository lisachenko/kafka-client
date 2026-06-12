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

namespace Protocol\Kafka\Protocol;


use Protocol\Kafka\IO\Stream;
/**
 * Kafka record class
 */
abstract class AbstractProtocolMessage implements BinarySchemaInterface
{
    /**
     * The message_size field gives the size of the subsequent request or response message in bytes.
     *
     * The client can read requests by first reading this 4 byte size as an integer N, and then reading and parsing
     * the subsequent N bytes of the request.
     *
     * @var integer
     */
    protected $messageSize = 0;

    /**
     * Unpacks the message from the binary data buffer
     *
     * @param Stream $stream Binary stream buffer
     *
     * @return static
     */
    final public static function unpack(Stream $stream)
    {
        return BinarySchema::readObjectFromStream(static::class, $stream);
    }

    /**
     * Writes the message to the stream
     *
     * @param Stream $stream Binary stream buffer
     */
    final public function writeTo(Stream $stream)
    {
        BinarySchema::writeObjectToStream($this, $stream);
    }
}
