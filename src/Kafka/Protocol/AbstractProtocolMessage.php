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

use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;

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
abstract class AbstractProtocolMessage implements BinarySchemaInterface
{
    /**
     * Upper bound for the size of one frame, mirroring the socket.request.max.bytes default of the broker.
     *
     * A size field larger than this can only come from a desynchronized connection.
     */
    private const int MAX_MESSAGE_SIZE = 104857600;

    /**
     * The message_size field gives the size of the subsequent request or response message in bytes.
     *
     * The client can read requests by first reading this 4 byte size as an integer N, and then reading and parsing
     * the subsequent N bytes of the request.
     */
    protected int $messageSize = 0;

    /**
     * A user-supplied integer value that will be passed back with the response (INT32)
     */
    protected int $correlationId = 0;

    /**
     * Unpacks the message from the binary data buffer.
     *
     * The announced frame is read in one go and parsed from an in-memory stream, so that a body parser can never
     * read past the boundary of its own message and desynchronize the connection.
     *
     * @param Stream $stream Binary stream buffer
     */
    final public static function unpack(Stream $stream): static
    {
        $messageSize = $stream->read('NmessageSize')['messageSize'];
        if ($messageSize < 0 || $messageSize > self::MAX_MESSAGE_SIZE) {
            throw new NetworkException(['error' => "Invalid message size received: {$messageSize}"]);
        }
        $payload = $messageSize > 0 ? (string) $stream->read("a{$messageSize}data")['data'] : '';

        // Protocol classes that have not been migrated to a scheme yet still parse their payload by hand
        if (static::hasLegacyPayloadReader()) {
            $self              = new static();
            $self->messageSize = $messageSize;
            static::unpackPayload($self, new StringStream($payload));

            return $self;
        }

        return BinarySchema::readObjectFromStream(
            static::class,
            new StringStream(pack('N', $messageSize) . $payload)
        );
    }

    /**
     * Writes the message to the stream
     *
     * @param Stream $stream Binary stream buffer
     */
    final public function writeTo(Stream $stream): void
    {
        $this->packInto($stream);
    }

    /**
     * Returns the binary message representation of the record
     */
    final public function __toString(): string
    {
        $stream = new StringStream();
        $this->packInto($stream);

        return $stream->getBuffer();
    }

    /**
     * Returns the size of the message, without the size field itself
     */
    final public function getMessageSize(): int
    {
        return $this->messageSize;
    }

    /**
     * Serializes this message into the given stream, size field included
     */
    protected function packInto(Stream $stream): void
    {
        BinarySchema::writeObjectToStream($this, $stream);
    }

    /**
     * Method to unpack the payload for the record.
     *
     * @param AbstractProtocolMessage|static $self   Instance of the current frame
     * @param Stream                         $stream Binary data of the frame, bounded by its announced size
     *
     * @deprecated Declare a {@see BinarySchemaInterface::getScheme()} instead, this hook disappears once every
     *             protocol class is described by a scheme.
     */
    protected static function unpackPayload(AbstractProtocolMessage $self, Stream $stream)
    {
        // nothing here
    }

    /**
     * Checks whether the concrete class still parses its payload by hand instead of declaring a scheme
     */
    private static function hasLegacyPayloadReader(): bool
    {
        return new \ReflectionMethod(static::class, 'unpackPayload')->getDeclaringClass()->getName() !== self::class;
    }
}
