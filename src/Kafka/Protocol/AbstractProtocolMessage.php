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
 * Common envelope for every request and response of the Kafka wire protocol.
 *
 * <pre>
 *   RequestOrResponse => Size (RequestMessage | ResponseMessage)
 *     Size => int32
 * </pre>
 *
 * @see docs/protocol/0.8.2.md, section "Common Request and Response Structure"
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
     */
    private string $messageData = '';

    /**
     * Unpacks the message from the binary data buffer
     *
     * @param Stream $stream Binary stream buffer
     *
     * @deprecated Use the typed {@see Request\AbstractResponse::unpackFrom()} contract for new protocol classes.
     */
    final public static function unpack(Stream $stream): static
    {
        $self              = new static();
        $self->messageSize = $stream->readInt32();
        if ($self->messageSize > 0) {
            static::unpackPayload($self, $stream);
        }

        return $self;
    }

    /**
     * Writes the message to the stream, prefixed with its int32 size
     *
     * @param Stream $stream Binary stream buffer
     */
    final public function writeTo(Stream $stream): void
    {
        $stream->writeBytes($this->messageData);
    }

    /**
     * Returns the binary message representation of record
     */
    final public function __toString(): string
    {
        return pack('N', $this->messageSize) . $this->messageData;
    }

    /**
     * Sets the content data and adjusts the length fields
     */
    final protected function setMessageData(string $data): void
    {
        $this->messageData = $data;
        $this->messageSize = strlen($this->messageData);
    }

    /**
     * Returns the context data from the record
     */
    final protected function getMessageData(): string
    {
        return $this->messageData;
    }

    /**
     * Returns the size of content length
     */
    final protected function getMessageSize(): int
    {
        return $this->messageSize;
    }

    /**
     * Method to unpack the payload for the record.
     *
     * NB: Default implementation will be always called
     *
     * @param AbstractProtocolMessage|static $self   Instance of current frame
     * @param Stream                         $stream Binary data
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, Stream $stream)
    {
        // nothing here
    }
}
