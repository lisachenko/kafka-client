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
 * ApiKeys record class
 */
class AbstractProtocolMessage implements \Stringable
{
    /**
     * The message_size field gives the size of the subsequent request or response message in bytes.
     *
     * The client can read requests by first reading this 4 byte size as an integer N, and then reading and parsing
     * the subsequent N bytes of the request.
     */
    private int $messageSize = 0;

    /**
     * The message_data field contains subsequent request or response message bytes.
     *
     * @var string
     */
    private $messageData = '';

    /**
     * Unpacks the message from the binary data buffer
     *
     * @param Stream $stream Binary stream buffer
     *
     * @return static
     */
    final public static function unpack(Stream $stream): static
    {
        $self = new static();
        $self->messageSize = $stream->read(ApiKeys::HEADER_FORMAT)['size'];
        if ($self->messageSize > 0) {
            static::unpackPayload($self, $stream);
        }

        return $self;
    }

    /**
     * Writes the message to the stream
     *
     * @param Stream $stream Binary stream buffer
     */
    final public function writeTo(Stream $stream): void
    {
        $stream->writeByteArray($this->messageData);
    }

    /**
     * Returns the binary message representation of record
     *
     * @return string
     */
    final public function __toString(): string
    {
        $headerPacket  = pack("N", $this->messageSize);
        return $headerPacket . $this->messageData;
    }

    /**
     * Sets the content data and adjusts the length fields
     *
     * @param $data
     */
    final protected function setMessageData($data)
    {
        $this->messageData = $data;
        $this->messageSize = strlen($this->messageData);
    }

    /**
     * Returns the context data from the record
     *
     * @return string
     */
    final protected function getMessageData()
    {
        return $this->messageData;
    }

    /**
     * Returns the size of content length
     *
     * @return int
     */
    final protected function getMessageSize()
    {
        return $this->messageSize;
    }

    /**
     * Method to unpack the payload for the record.
     *
     * NB: Default implementation will be always called
     *
     * @param AbstractProtocolMessage|static $self   Instance of current frame
     * @param Stream $stream Binary data
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, Stream $stream)
    {
        // nothing here
    }
}
