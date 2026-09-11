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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One partition of a Fetch response
 *
 * <pre>
 *   FetchResponsePartition => Partition ErrorCode HighwaterMarkOffset LastStableOffset LogStartOffset
 *                             [AbortedTransactions] RecordSetSize RecordSet
 *     Partition            => int32
 *     ErrorCode            => int16
 *     HighwaterMarkOffset  => int64
 *     LastStableOffset     => int64
 *     LogStartOffset       => int64
 *     AbortedTransactions  => nullable [ProducerId int64 FirstOffset int64]
 *     RecordSetSize        => int32
 * </pre>
 *
 * The record set is read as a byte array, because `MessageSetSize`/`RecordSetSize` is exactly the int32 length
 * prefix of the BYTEARRAY type; decoding those bytes into batches and records is the job of the record layer, i.e.
 * of {@see MemoryRecords}, which reads whichever of the three message formats the broker answered with.
 *
 * The partition entry is the same in the versions 0 to 3. Version 4 (Kafka 0.11.0, KIP-98) added
 * `LastStableOffset` and the nullable `AbortedTransactions` array behind the high water mark, and version 5
 * (KIP-107) added `LogStartOffset` between the two, which is what {@see FetchResponsePartitionV4} and
 * {@see FetchResponsePartitionV0} lower the version constant for.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v11)", "MessageSet and Message" and
 *      "RecordBatch (message format v2)"
 */
class FetchResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO is unpacked from
     */
    public const int VERSION = 11;

    /**
     * Value of `LastStableOffset` in an answer that does not carry the field, and of a `read_uncommitted` fetch
     */
    public const int INVALID_LAST_STABLE_OFFSET = -1;

    /**
     * Value of `LogStartOffset` in an answer of a version below 5, which does not carry the field at all
     */
    public const int INVALID_LOG_START_OFFSET = -1;

    /**
     * Value of {@see self::$preferredReadReplica} that names no replica at all: "read from the leader" (KIP-392)
     */
    public const int NO_PREFERRED_READ_REPLICA = -1;

    /**
     * The id of the partition this response is for.
     */
    public int $partition;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have been fetched successfully.
     */
    public int $errorCode;

    /**
     * The offset at the end of the log for this partition.
     *
     * This can be used by the client to determine how many messages behind the end of the log they are.
     */
    public int $highWaterMarkOffset;

    /**
     * The last stable offset (LSO) of the partition, the offset every transaction below which has been decided.
     *
     * A `read_committed` fetch stops at this offset instead of at the high water mark: everything below it is
     * either non-transactional or belongs to a transaction that has been committed or aborted, so a consumer can
     * decide what to hand to the application. Without any transaction on the partition the LSO **is** the high
     * water mark, and a `read_uncommitted` fetch is answered with
     * {@see self::INVALID_LAST_STABLE_OFFSET} - the broker does not compute the value it would not use.
     *
     * @since Version 4 of protocol
     */
    public int $lastStableOffset = self::INVALID_LAST_STABLE_OFFSET;

    /**
     * Earliest offset that is still on disk for this partition (KIP-107).
     *
     * A consumer learns from it that the beginning of the log moved - by the retention of the broker or by a
     * `DeleteRecords` call - without asking the Offsets api for the earliest offset. A partition that was never
     * truncated answers 0; an answer of a version below 5 leaves the field at
     * {@see self::INVALID_LOG_START_OFFSET}.
     *
     * @since Version 5 of protocol
     */
    public int $logStartOffset = self::INVALID_LOG_START_OFFSET;

    /**
     * Transactions that were aborted in the range this answer covers, `null` for a `read_uncommitted` fetch.
     *
     * The distinction matters: `null` means "the broker was not asked to track transactions", an **empty array**
     * means "asked, and nothing was aborted here". Both are answered by a real broker, see
     * {@see FetchResponseAbortedTransaction}.
     *
     * @var list<FetchResponseAbortedTransaction>|null
     *
     * @since Version 4 of protocol
     */
    public ?array $abortedTransactions = null;

    /**
     * Replica the consumer should read this partition from next, `-1` when that is the leader itself.
     *
     * **KIP-392** (Kafka 2.3) lets a consumer read from a **follower** instead of the leader, to keep the traffic
     * of a rack-aware cluster inside its rack. The consumer names its own rack in the `rack_id` of the request
     * ({@see \Protocol\Kafka\Protocol\Request\FetchRequest::$rackId},
     * {@see \Protocol\Kafka\Consumer\ConsumerConfig::CLIENT_RACK}), and the **leader** decides: its
     * `replica.selector.class` picks a replica for that rack and answers its node id here. The consumer then
     * fetches the partition from that broker until the answer names another one - the field travels in every
     * answer, so a leader can take the reader back at any time.
     *
     * {@see self::NO_PREFERRED_READ_REPLICA} (`-1`) is "read from me", which is what a broker without a selector
     * (the default `replica.selector.class` is unset, and so is the container's) answers to every fetch, and what
     * an answer below version 11 leaves here.
     *
     * @since Version 11 of protocol
     */
    public int $preferredReadReplica = self::NO_PREFERRED_READ_REPLICA;

    /**
     * Raw bytes of the returned record set, exactly as they lie in the log.
     *
     * The broker is allowed to cut the last batch of the set short, therefore these bytes are not necessarily a
     * sequence of complete batches.
     */
    public ?string $messageSet = null;

    /**
     * Lazily decoded record region of this partition
     */
    private ?MemoryRecords $decodedRecords = null;

    /**
     * Lazily decoded legacy message set of this partition
     */
    private ?MessageSet $decodedMessageSet = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition'           => BinarySchema::TYPE_INT32,
            'errorCode'           => BinarySchema::TYPE_INT16,
            'highWaterMarkOffset' => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 4) {
            $scheme['lastStableOffset'] = BinarySchema::TYPE_INT64;
        }
        if (static::VERSION >= 5) {
            $scheme['logStartOffset'] = BinarySchema::TYPE_INT64;
        }
        if (static::VERSION >= 4) {
            $scheme['abortedTransactions'] = [
                FetchResponseAbortedTransaction::class,
                BinarySchema::FLAG_NULLABLE => true,
            ];
        }
        if (static::VERSION >= 11) {
            $scheme['preferredReadReplica'] = BinarySchema::TYPE_INT32;
        }
        $scheme['messageSet'] = BinarySchema::TYPE_BYTEARRAY;

        return $scheme;
    }

    /**
     * Decodes the raw bytes of this partition into a record region, dropping a partial trailing batch.
     *
     * The bytes are decoded once and the result is kept, because a fetch loop asks for the records of a partition
     * and for their offsets separately. Which message format the bytes are in is decided by the log and by the api
     * version the broker converted them for, not by this class: {@see MemoryRecords::fromBuffer()} reads all three.
     */
    public function getRecords(): MemoryRecords
    {
        return $this->decodedRecords ??= MemoryRecords::fromBuffer($this->messageSet ?? '');
    }

    /**
     * Decodes the raw bytes of this partition into a legacy message set, dropping a partial trailing message.
     *
     * This is what an answer of the versions 0 to 3 holds - the broker converts a v2 log down to the message
     * format v1 or v0 for those - and it throws on a record batch of the message format v2, which only
     * {@see self::getRecords()} understands.
     */
    public function getMessageSet(): MessageSet
    {
        return $this->decodedMessageSet ??= MessageSet::fromBuffer($this->messageSet ?? '');
    }

    /**
     * Tells whether a single message of this partition is bigger than the MaxBytes that were asked for.
     *
     * **This is the answer of a Fetch request below version 3 only.** Up to version 2 the broker cuts the message
     * set off at `MaxBytes` and guarantees no progress: when the message at `FetchOffset` is bigger than that
     * limit, the partition comes back without an error and with a message set that holds no complete message at
     * all - either nothing or the first bytes of that one message - while its high water mark shows that there is
     * something to read. A consumer that keeps fetching the same offset would spin forever, so it has to raise
     * `max.partition.fetch.bytes` instead.
     *
     * Version 3 (KIP-74) removed that state: the first non-empty partition of an answer ignores both size limits
     * and returns at least one complete message, and an incomplete set is replaced with an empty one
     * (`ReplicaManager.readFromLocalLog` @ 0.10.2.2: `!hardMaxBytesLimit && fetch.firstEntryIncomplete`). An empty
     * partition below the high water mark is then perfectly normal - the budget of the answer was used up by the
     * partitions in front of it - so this question must not be asked about a version 3 answer, which is why
     * {@see \Protocol\Kafka\Client::fetchPartitions()} only asks it for the lower versions.
     *
     * @param int $fetchOffset The offset that was requested for this partition
     */
    public function isSingleMessageTooLarge(int $fetchOffset): bool
    {
        return $this->errorCode === 0
            && !$this->hasCompleteMessage()
            && $this->highWaterMarkOffset > $fetchOffset;
    }

    /**
     * Checks whether the returned bytes begin with at least one complete entry.
     *
     * The check is the same for all three message formats: the first twelve bytes of an entry are the offset and
     * the size of what follows them - `Offset`/`MessageSize` of a message, `BaseOffset`/`BatchLength` of a record
     * batch - so the entry is complete when that many bytes are there.
     */
    private function hasCompleteMessage(): bool
    {
        $buffer = $this->messageSet ?? '';
        if (strlen($buffer) < 12 /* Offset int64 + MessageSize int32 */) {
            return false;
        }
        $messageSize = (int) unpack('NmessageSize', $buffer, 8)['messageSize'];

        return strlen($buffer) >= 12 + $messageSize;
    }
}
