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

namespace Protocol\Kafka\Common\Record;

use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\IO\StringStream;

/**
 * The message set structure, shared by the Produce and the Fetch APIs and by the on-disk log of the broker.
 *
 * <pre>
 *   MessageSet => [Offset MessageSize Message]
 *     Offset      => int64
 *     MessageSize => int32
 * </pre>
 *
 * A message set is *not* a regular protocol array: it has no int32 element count in front of it, it is a raw byte
 * region whose length is given by the MessageSetSize field of the partition that carries it. This is the 0.8
 * counterpart of the RecordBatch of the later protocol lines.
 *
 * Two properties of the format shape the reader:
 *
 *  * the broker is allowed to return a **partial message** at the end of a set, which has to be dropped silently;
 *  * a **compressed** message set is a set of exactly one message whose Value is a complete, compressed message set,
 *    so reading a buffer unwraps those inner sets and keeps the offsets that the broker assigned to the inner
 *    messages.
 *
 * @see docs/protocol/0.9.0.md, section "MessageSet and Message"
 * @see kafka/message/ByteBufferMessageSet.scala @ 0.9.0.1
 */
final class MessageSet implements \Countable, \Stringable
{
    /**
     * Size of the Offset and MessageSize fields that precede every message of a set
     */
    public const int ENTRY_OVERHEAD = 8 + 4;

    /**
     * Entries of the set, each one an offset and the message stored at it
     *
     * @param list<array{0: int, 1: Message}> $entries
     * @param bool $partialTrailingMessage Whether the buffer this set was read from ended inside a message
     */
    private function __construct(
        private readonly array $entries,
        private readonly bool $partialTrailingMessage = false,
    ) {}

    /**
     * Builds a message set out of user-facing records, optionally compressing it as a whole.
     *
     * The offsets of a produced set are ignored by the broker, which assigns the real ones on append, so they simply
     * count from 0. A compressed set becomes a single wrapper message whose offset is the one of the last inner
     * message, the way `ByteBufferMessageSet.create()` writes it.
     *
     * @param iterable<Record> $records
     * @param int              $compressionCodec One of the {@see CompressionCodec} constants
     */
    public static function fromRecords(iterable $records, int $compressionCodec = CompressionCodec::NONE): self
    {
        $entries = [];
        $offset  = 0;
        foreach ($records as $record) {
            // The codec of a set is announced by the wrapper message only, never by the messages inside it
            $attributes = $record->attributes & ~CompressionCodec::MASK;
            $entries[]  = [$offset++, new Message($record->value, $record->key, $attributes)];
        }

        if ($entries === [] || $compressionCodec === CompressionCodec::NONE) {
            return new self($entries);
        }

        $lastOffset = $entries[count($entries) - 1][0];
        $wrapper    = Message::compressed(new self($entries)->toBuffer(), $compressionCodec);

        return new self([[$lastOffset, $wrapper]]);
    }

    /**
     * Reads a message set out of the raw byte region that a produce or a fetch partition carries.
     *
     * A message that the buffer does not hold completely is dropped, as the specification requires; the fact that it
     * was there is kept in {@see MessageSet::hasPartialTrailingMessage()} so that a caller can tell an empty answer
     * from an answer that did not fit into the requested amount of bytes.
     *
     * @param bool $checkCrcs Whether to validate the checksum of every message, the `check.crcs` behaviour of the
     *                        official clients
     *
     * @throws CorruptMessageException on a message with an impossible size or a mismatching checksum
     */
    public static function fromBuffer(string $buffer, bool $checkCrcs = true): self
    {
        $entries      = [];
        $bufferLength = strlen($buffer);
        $position     = 0;
        $isPartial    = false;

        while ($position < $bufferLength) {
            if ($bufferLength - $position < self::ENTRY_OVERHEAD) {
                $isPartial = true;
                break;
            }

            /** @var array{offset: int, messageSize: int} $header */
            $header      = unpack('Joffset/NmessageSize', $buffer, $position);
            $offset      = $header['offset'];
            $messageSize = $header['messageSize'];
            if ($messageSize >= 0x80000000) {
                $messageSize -= 0x100000000;
            }
            if ($messageSize < Message::MIN_SIZE) {
                throw new CorruptMessageException([
                    'error'       => 'A message of the set announced a size below the minimum message size',
                    'offset'      => $offset,
                    'messageSize' => $messageSize,
                ]);
            }
            if ($bufferLength - $position - self::ENTRY_OVERHEAD < $messageSize) {
                // The broker may cut the last message of a set short, it has to be dropped silently
                $isPartial = true;
                break;
            }

            $position += self::ENTRY_OVERHEAD;
            $message = Message::fromBuffer(substr($buffer, $position, $messageSize), $checkCrcs);
            $position += $messageSize;

            if (!$message->isCompressed()) {
                $entries[] = [$offset, $message];

                continue;
            }

            // A compressed message wraps a whole message set, whose inner offsets are the ones to report
            $innerSet = self::fromBuffer($message->decompressValue(), $checkCrcs);
            foreach ($innerSet->entries as $innerEntry) {
                $entries[] = $innerEntry;
            }
            $isPartial = $isPartial || $innerSet->partialTrailingMessage;
        }

        return new self($entries, $isPartial);
    }

    /**
     * Serializes the message set into the raw byte region that a partition of a request or a response carries
     */
    public function toBuffer(): string
    {
        $stream = new StringStream();
        foreach ($this->entries as [$offset, $message]) {
            $messageBuffer = $message->toBuffer();
            $stream->write('JN', $offset, strlen($messageBuffer));
            $stream->writeBuffer($messageBuffer);
        }

        return $stream->getBuffer();
    }

    /**
     * Returns the size of the serialized message set in bytes, the MessageSetSize field of the partition.
     *
     * For a set that was read from a compressed buffer this is the size of the *unwrapped* set, which is what
     * {@see MessageSet::toBuffer()} would produce, not the size of the compressed bytes it was read from.
     */
    public function sizeInBytes(): int
    {
        $size = 0;
        foreach ($this->entries as [, $message]) {
            $size += self::ENTRY_OVERHEAD + $message->sizeInBytes();
        }

        return $size;
    }

    /**
     * Returns the records of the set, with the offset that the broker assigned to each of them
     *
     * @return list<Record>
     */
    public function getRecords(): array
    {
        $records = [];
        foreach ($this->entries as [$offset, $message]) {
            $records[] = new Record($message->value, $message->key, $message->attributes, $offset);
        }

        return $records;
    }

    /**
     * Returns the raw entries of the set, each one the offset and the message stored at it
     *
     * @return list<array{0: int, 1: Message}>
     */
    public function getMessages(): array
    {
        return $this->entries;
    }

    /**
     * Tells whether the buffer that this set was read from ended in the middle of a message.
     *
     * A fetch that comes back empty and partial means that the first message of the partition is larger than the
     * amount of bytes the request allowed for it, and that a bigger MaxBytes is needed to make progress.
     */
    public function hasPartialTrailingMessage(): bool
    {
        return $this->partialTrailingMessage;
    }

    /**
     * Returns the number of messages in the set, inner messages of a compressed set included
     */
    public function count(): int
    {
        return count($this->entries);
    }

    /**
     * Tells whether the set holds no message at all
     */
    public function isEmpty(): bool
    {
        return $this->entries === [];
    }

    public function __toString(): string
    {
        return $this->toBuffer();
    }
}
