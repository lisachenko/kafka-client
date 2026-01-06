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
 * @date 14.07.2014
 */

namespace Protocol\Kafka\Protocol;

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
     * @param string $data Binary buffer with raw data
     *
     * @return static
     */
    final public static function unpack($data): static
    {
        $self = new static();
        [$self->messageSize] = array_values(unpack(ApiKeys::HEADER_FORMAT, $data));

        $payload = substr($data, ApiKeys::HEADER_LEN);
        self::unpackPayload($self, $payload);
        if (static::class !== self::class && $self->messageSize > 0) {
            static::unpackPayload($self, $self->messageData);
        }

        return $self;
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
    public function setMessageData($data): void
    {
        $this->messageData = $data;
        $this->messageSize = strlen($this->messageData);
    }

    /**
     * Returns the context data from the record
     *
     * @return string
     */
    public function getMessageData()
    {
        return $this->messageData;
    }

    /**
     * Returns the size of content length
     *
     * @return int
     */
    final public function getMessageSize()
    {
        return $this->messageSize;
    }

    /**
     * Method to unpack the payload for the record.
     *
     * NB: Default implementation will be always called
     *
     * @param AbstractProtocolMessage|static $self Instance of current frame
     * @param string $data Binary data
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, $data)
    {
        [$self->messageData] = array_values(unpack("a{$self->messageSize}contentData", $data));
    }

    /**
     * Implementation of packing the payload
     *
     * @return string
     */
    protected function packPayload(): string
    {
        return pack("a{$this->messageSize}", $this->messageData);
    }
}
