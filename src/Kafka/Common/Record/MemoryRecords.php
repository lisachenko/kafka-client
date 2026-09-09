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

/**
 * The byte region that a Produce or a Fetch partition carries, in whichever message format it happens to be in.
 *
 * A 0.11 broker stores and serves all three message formats: the log of a topic is written in its
 * `message.format.version`, and a fetch of an old client is converted down to the format that client understands.
 * A client therefore cannot assume a format - it has to look at the bytes, which is what this class does and what
 * `MemoryRecords` of the Java client does.
 *
 * The three formats are laid out so that the discriminator sits at the **same offset in all of them**:
 *
 * <pre>
 *   bytes  0- 7   int64   the offset of a message (v0, v1) or the baseOffset of a batch (v2)
 *   bytes  8-11   int32   the size of the message that follows (v0, v1) or the batchLength (v2)
 *   bytes 12-15   int32   the CRC of the message (v0, v1) or the partitionLeaderEpoch of the batch (v2)
 *   byte     16   int8    the magic byte, in every format
 * </pre>
 *
 * A reader walks the region entry by entry, reads the length, looks at byte 16 of the entry and hands the bytes to
 * {@see MessageSet} for the magic 0 and 1 or to {@see RecordBatch} for the magic 2. Every entry is one *batch*: a
 * legacy entry holds a single message, which is a whole compressed message set when it announces a codec, and a v2
 * entry holds a record batch.
 *
 * The last entry of a region may be **cut short** by the `MaxBytes` of the fetch. It is dropped silently, and
 * {@see MemoryRecords::hasPartialTrailingRecord()} keeps the fact that it was there, so that a caller can tell an
 * empty answer from an answer that did not fit.
 *
 * {@see MemoryRecords::getRecords()} answers what an application is allowed to see: the records of every batch,
 * without the **control batches**, whose records are markers of the transaction protocol and never leave the client.
 *
 * @see docs/protocol/0.11.0.md, section "RecordBatch (message format v2)"
 * @see org/apache/kafka/common/record/MemoryRecords.java @ 0.11.0.3
 */
final class MemoryRecords implements \Countable, \Stringable
{
    /**
     * Size of the two fields that precede every entry of a region, in all three message formats
     */
    public const int LOG_OVERHEAD = RecordBatch::LOG_OVERHEAD;

    /**
     * Position of the magic byte inside an entry, the same one in all three message formats
     */
    public const int MAGIC_OFFSET = RecordBatch::MAGIC_OFFSET;

    /**
     * @param list<array{0: MessageSet|RecordBatch, 1: list<Record>}> $entries      The batches of the region and the
     *                                                                              records each of them holds
     * @param string                                                 $buffer       The bytes the region was read from
     * @param bool                                                   $partial      Whether the region ended inside a
     *                                                                              batch
     */
    private function __construct(
        private readonly array $entries,
        private readonly string $buffer,
        private readonly bool $partial = false,
    ) {}

    /**
     * Reads a byte region of any of the three message formats.
     *
     * @param bool $checkCrcs Whether to validate the checksum of every batch, the `check.crcs` behaviour of the
     *                        official clients
     *
     * @throws CorruptMessageException on a batch with an impossible size, an unknown magic or a mismatching checksum
     */
    public static function fromBuffer(string $buffer, bool $checkCrcs = true): self
    {
        $entries      = [];
        $bufferLength = strlen($buffer);
        $position     = 0;
        $isPartial    = false;

        while ($position < $bufferLength) {
            $remaining = $bufferLength - $position;
            if ($remaining < self::LOG_OVERHEAD) {
                $isPartial = true;
                break;
            }

            /** @var array{length: int} $header */
            $header = unpack('Nlength', $buffer, $position + 8);
            $length = $header['length'];
            if ($length >= 0x80000000) {
                $length -= 0x100000000;
            }
            // The smallest entry of any format is a message of format v0 without a key and without a value
            if ($length < Message::MIN_SIZE_V0) {
                throw new CorruptMessageException([
                    'error'  => 'An entry of a record region announces a size below the smallest possible one',
                    'length' => $length,
                ]);
            }
            if ($remaining < self::LOG_OVERHEAD + $length) {
                // The broker may cut the last batch of a region short, it has to be dropped silently
                $isPartial = true;
                break;
            }

            $entryBytes = substr($buffer, $position, self::LOG_OVERHEAD + $length);
            $position += self::LOG_OVERHEAD + $length;

            $magic = ord($entryBytes[self::MAGIC_OFFSET]);
            if ($magic === RecordBatch::MAGIC) {
                $batch     = RecordBatch::fromBuffer($entryBytes, $checkCrcs);
                $entries[] = [$batch, $batch->isControlBatch() ? [] : $batch->getRecords()];

                continue;
            }

            $entries[] = [
                MessageSet::shallowFromBuffer($entryBytes, $checkCrcs),
                MessageSet::fromBuffer($entryBytes, $checkCrcs)->getRecords(),
            ];
        }

        return new self($entries, $buffer, $isPartial);
    }

    /**
     * Wraps a single record batch, the way a produce request of the message format v2 carries it
     */
    public static function fromRecordBatch(RecordBatch $batch): self
    {
        return new self(
            [[$batch, $batch->isControlBatch() ? [] : $batch->getRecords()]],
            $batch->toBuffer()
        );
    }

    /**
     * Wraps a legacy message set, the way a produce request of the message formats v0 and v1 carries it
     */
    public static function fromMessageSet(MessageSet $messageSet): self
    {
        if ($messageSet->isEmpty()) {
            return new self([], '');
        }

        return new self([[$messageSet, $messageSet->getRecords()]], $messageSet->toBuffer());
    }

    /**
     * Returns the records of the region that an application is allowed to see, in the order of the log.
     *
     * The records of a **control batch** are not among them: they are markers of the transaction protocol, and
     * `Clients should not return control batches to applications` is a rule of the format itself.
     *
     * @return list<Record>
     */
    public function getRecords(): array
    {
        $records = [];
        foreach ($this->entries as [, $batchRecords]) {
            foreach ($batchRecords as $record) {
                $records[] = $record;
            }
        }

        return $records;
    }

    /**
     * Returns the batches of the region as they are on the wire: a {@see MessageSet} of one entry for the message
     * formats v0 and v1, a {@see RecordBatch} for the format v2 - control batches included
     *
     * @return list<MessageSet|RecordBatch>
     */
    public function getBatches(): array
    {
        return array_map(
            static fn(array $entry): MessageSet|RecordBatch => $entry[0],
            $this->entries
        );
    }

    /**
     * Returns the bytes of the region, byte for byte the ones it was read from
     */
    public function toBuffer(): string
    {
        return $this->buffer;
    }

    /**
     * Returns the size of the region in bytes
     */
    public function sizeInBytes(): int
    {
        return strlen($this->buffer);
    }

    /**
     * Tells whether the region ended in the middle of a batch.
     *
     * A fetch that comes back empty and partial means that the first batch of the partition is larger than the
     * amount of bytes the request allowed for it, and that a bigger MaxBytes is needed to make progress.
     */
    public function hasPartialTrailingRecord(): bool
    {
        return $this->partial;
    }

    /**
     * Returns the message format of the region: the magic byte of its first batch, null for an empty region
     */
    public function getMagic(): ?int
    {
        if ($this->entries === []) {
            return null;
        }
        $first = $this->entries[0][0];

        return $first instanceof RecordBatch ? $first->magic : $first->getMagic();
    }

    /**
     * Tells whether the region holds no batch at all
     */
    public function isEmpty(): bool
    {
        return $this->entries === [];
    }

    /**
     * Returns the number of batches of the region; the number of records is the size of {@see self::getRecords()}
     */
    public function count(): int
    {
        return count($this->entries);
    }

    public function __toString(): string
    {
        return $this->buffer;
    }
}
