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
 * Three properties of the format shape the reader:
 *
 *  * the broker is allowed to return a **partial message** at the end of a set, which has to be dropped silently;
 *  * a **compressed** message set is a set of exactly one message whose Value is a complete, compressed message set,
 *    so reading a buffer unwraps those inner sets and keeps the offsets that the broker assigned to the inner
 *    messages;
 *  * in **message format v1** the offsets inside a compressed set are **relative** to the wrapper: the wrapper
 *    carries the absolute offset of the last inner message and the inner offsets count from 0, so the absolute
 *    offset of an inner message is `wrapperOffset - lastInnerOffset + innerOffset`. Message format v0 stores the
 *    absolute offsets inside the wrapper, which is why the reader looks at the magic byte of the wrapper before it
 *    interprets them.
 *
 * The timestamp of an inner message follows the same rule as in `RecordsIterator.DeepRecordsIterator`: the timestamp
 * type of the wrapper applies to every message of the set, and a `LogAppendTime` wrapper replaces the timestamps of
 * all of its inner messages with its own.
 *
 * The message formats v0 and v1 have no place for the **headers** of a record (KIP-82): a header only exists in the
 * message format v2, so {@see MessageSet::fromRecords()} silently ignores the headers of the records it is given and
 * {@see MessageSet::getRecords()} never fills any. A byte region of an unknown format is read by
 * {@see MemoryRecords}, which dispatches on the magic byte of every entry; this class refuses a magic 2.
 *
 * @see docs/protocol/2.8.md, sections "MessageSet and Message" and "RecordBatch (message format v2)"
 * @see kafka/message/ByteBufferMessageSet.scala @ 0.10.2.2
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
     * @param list<array{0: int, 1: Message}>  $entries
     * @param list<array{0: ?int, 1: int}>     $timestamps Timestamp and timestamp type of every entry, in the order
     *                                                    of the entries; an inner message of a compressed set takes
     *                                                    them from its wrapper
     * @param bool $partialTrailingMessage Whether the buffer this set was read from ended inside a message
     */
    private function __construct(
        private readonly array $entries,
        private readonly array $timestamps = [],
        private readonly bool $partialTrailingMessage = false,
    ) {}

    /**
     * Builds a message set out of user-facing records, optionally compressing it as a whole.
     *
     * The offsets of a produced set are ignored by the broker, which assigns the real ones on append, so they simply
     * count from 0 - which makes them the relative offsets of message format v1 and the absolute ones of v0 at the
     * same time. A compressed set becomes a single wrapper message whose offset is the one of the last inner
     * message, the way `MemoryRecordsBuilder` writes it; in message format v1 that wrapper also carries the largest
     * timestamp of the set and the `CreateTime` timestamp type, which is the only type a producer ever writes.
     *
     * The headers of a record are ignored here: neither of the two message formats of a message set can carry them,
     * only the {@see RecordBatch} of the message format v2 can.
     *
     * @param iterable<Record> $records
     * @param int              $compressionCodec One of the {@see CompressionCodec} constants
     * @param int              $magic            Message format to write, {@see Message::MAGIC_V1} by default
     */
    public static function fromRecords(
        iterable $records,
        int $compressionCodec = CompressionCodec::NONE,
        int $magic = Message::MAGIC_V1
    ): self {
        $entries          = [];
        $timestamps       = [];
        $offset           = 0;
        $largestTimestamp = Message::NO_TIMESTAMP;

        foreach ($records as $record) {
            // The codec of a set is announced by the wrapper message only, never by the messages inside it, and a
            // produced message always carries the CreateTime type - the broker rejects anything else
            $attributes = TimestampType::updateAttributes(
                $record->attributes & ~CompressionCodec::MASK,
                TimestampType::CREATE_TIME
            );
            $timestamp        = $magic === Message::MAGIC_V0 ? null : $record->timestamp;
            $largestTimestamp = max($largestTimestamp, $timestamp ?? Message::NO_TIMESTAMP);

            $entries[] = [
                $offset++,
                Message::ofMagic($magic, $record->value, $record->key, $attributes, $timestamp ?? Message::NO_TIMESTAMP),
            ];
            $timestamps[] = [
                $timestamp,
                $magic === Message::MAGIC_V0 ? TimestampType::NO_TIMESTAMP_TYPE : TimestampType::CREATE_TIME,
            ];
        }

        if ($entries === [] || $compressionCodec === CompressionCodec::NONE) {
            return new self($entries, $timestamps);
        }

        $lastOffset   = $entries[count($entries) - 1][0];
        $wrapperClass = Message::classOfMagic($magic);
        $wrapper      = $wrapperClass::compressed(
            new self($entries, $timestamps)->toBuffer(),
            $compressionCodec,
            $largestTimestamp,
            TimestampType::CREATE_TIME
        );

        return new self(
            [[$lastOffset, $wrapper]],
            [[$wrapper->getTimestamp(), $wrapper->getTimestampType()]]
        );
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
        return self::read($buffer, $checkCrcs, true);
    }

    /**
     * Reads the outer messages of a byte region without decompressing anything.
     *
     * This is the shallow iteration of the broker: a compressed set stays the single wrapper message it is on the
     * wire, with the offset and the timestamp the log holds for it. It is what a caller needs to look at the format
     * of a stored set instead of at the records inside it, and the only way to write the very same bytes back out
     * with {@see MessageSet::toBuffer()}.
     *
     * @throws CorruptMessageException on a message with an impossible size or a mismatching checksum
     */
    public static function shallowFromBuffer(string $buffer, bool $checkCrcs = true): self
    {
        return self::read($buffer, $checkCrcs, false);
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
     * Returns the records of the set, with the offset and the timestamp that the broker assigned to each of them
     *
     * @return list<Record>
     */
    public function getRecords(): array
    {
        $records = [];
        foreach ($this->entries as $index => [$offset, $message]) {
            [$timestamp, $timestampType] = $this->timestamps[$index] ?? [null, TimestampType::NO_TIMESTAMP_TYPE];

            $records[] = new Record(
                $message->value,
                $message->key,
                $message->attributes,
                $offset,
                $timestamp,
                $timestampType
            );
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
     * Returns the message format of the set: the magic byte of its first message, null for an empty set
     */
    public function getMagic(): ?int
    {
        if ($this->entries === []) {
            return null;
        }

        return $this->entries[0][1]->magicByte;
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

    /**
     * Reads a byte region, either unwrapping the compressed sets it holds or keeping their wrapper messages
     *
     * @param bool $deep Whether to decompress a wrapper message into the messages it holds
     *
     * @throws CorruptMessageException on a message with an impossible size or a mismatching checksum
     */
    private static function read(string $buffer, bool $checkCrcs, bool $deep): self
    {
        $entries      = [];
        $timestamps   = [];
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
            // The smallest message of any format is one of format v0 without a key and without a value
            if ($messageSize < Message::MIN_SIZE_V0) {
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

            if (!$deep || !$message->isCompressed()) {
                $entries[]    = [$offset, $message];
                $timestamps[] = [$message->getTimestamp(), $message->getTimestampType()];

                continue;
            }

            // A compressed message wraps a whole message set; nested compression does not exist, so the inner set
            // is read shallowly and its offsets are translated into the absolute ones of the log
            $innerSet        = self::read($message->decompressValue(), $checkCrcs, false);
            $innerTimestamps = $innerSet->timestampsOfWrapper($message);
            foreach (self::unwrap($offset, $message, $innerSet) as $index => $innerEntry) {
                $entries[]    = $innerEntry;
                $timestamps[] = $innerTimestamps[$index];
            }
            $isPartial = $isPartial || $innerSet->partialTrailingMessage;
        }

        return new self($entries, $timestamps, $isPartial);
    }

    /**
     * Translates the offsets of the inner messages of a compressed set into the absolute offsets of the log
     *
     * Message format v1 stores them relative to the wrapper, which carries the absolute offset of the *last* inner
     * message: `absolute = wrapperOffset - lastInnerOffset + innerOffset`. Message format v0 stores the absolute
     * offsets themselves and needs no translation at all.
     *
     * @return list<array{0: int, 1: Message}>
     */
    private static function unwrap(int $wrapperOffset, Message $wrapper, self $innerSet): array
    {
        $innerEntries = $innerSet->entries;
        if ($wrapper->magicByte === Message::MAGIC_V0 || $innerEntries === []) {
            return $innerEntries;
        }

        $lastInnerOffset = $innerEntries[count($innerEntries) - 1][0];
        $baseOffset      = $wrapperOffset - $lastInnerOffset;

        $absolute = [];
        foreach ($innerEntries as [$innerOffset, $innerMessage]) {
            $absolute[] = [$baseOffset + $innerOffset, $innerMessage];
        }

        return $absolute;
    }

    /**
     * Returns the timestamp and the timestamp type of every inner message of the given wrapper.
     *
     * The timestamp type of the wrapper applies to the whole set, and a `LogAppendTime` wrapper also replaces the
     * timestamps of its inner messages with its own - those are the two rules of `Record.timestamp()` and
     * `Record.timestampType()` of the Java client.
     *
     * @return list<array{0: ?int, 1: int}>
     */
    private function timestampsOfWrapper(Message $wrapper): array
    {
        $wrapperTimestampType = $wrapper->getTimestampType();
        $wrapperTimestamp     = $wrapper->getTimestamp();

        $timestamps = [];
        foreach ($this->entries as $index => [, $innerMessage]) {
            if ($wrapperTimestampType === TimestampType::LOG_APPEND_TIME) {
                $timestamps[] = [$wrapperTimestamp, TimestampType::LOG_APPEND_TIME];

                continue;
            }
            $timestamps[] = [$this->timestamps[$index][0], $wrapperTimestampType];
        }

        return $timestamps;
    }
}
