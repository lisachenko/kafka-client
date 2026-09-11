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
 * Every message also knows **which version of the api it stands for** and whether that version is a flexible one
 * (KIP-482, Kafka 2.4): `VERSION` is the version in the header of a request and the version the answer was read
 * with, `FLEXIBLE_VERSION` is the first version of the api whose frame is written with the compact types and the
 * tagged fields, and {@see self::isFlexible()} is the question the schema engine asks once per message. A class
 * that does not override `FLEXIBLE_VERSION` is never flexible, which is what every api of the lines below this one
 * is.
 *
 * @see docs/protocol/2.8.md, sections "Common request and response structure" and "Implementation model"
 */
abstract class AbstractProtocolMessage implements FlexibleSchemaInterface
{
    use PreservesUnknownTaggedFields;

    /**
     * Version of the api this class stands for (INT16), overridden by every versioned subclass
     */
    public const int VERSION = 0;

    /**
     * First version of this api that is **flexible**, i.e. written with the compact types and the tagged fields of
     * KIP-482 (the `flexibleVersions` of its JSON message specification @ 2.8.2)
     *
     * `PHP_INT_MAX` means "no version of this api is flexible", which is the answer for every api of Kafka 1.1.1
     * and below, for SaslHandshake (17) and for OffsetDelete (47).
     */
    public const int FLEXIBLE_VERSION = PHP_INT_MAX;

    /**
     * Tagged fields of the **header** of this message, as `tag => raw bytes`
     *
     * The request header v2 and the response header v1 end in a tagged-field section of their own, in the middle of
     * the frame; no api of Kafka 2.8.2 defines a tag for it, so this is the empty array on every frame this client
     * writes and on every frame it has seen. It is a property of its own rather than part of
     * {@see PreservesUnknownTaggedFields} because the header and the body are two structures with two sections.
     *
     * @var array<int, string>
     */
    protected array $headerTaggedFields = [];
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
     * Whether the version this class stands for is written with the compact types and tagged fields of KIP-482
     */
    final public static function isFlexible(): bool
    {
        return static::VERSION >= static::FLEXIBLE_VERSION;
    }

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
}
