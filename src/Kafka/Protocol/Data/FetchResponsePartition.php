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

use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One partition of a Fetch response
 *
 * <pre>
 *   FetchResponsePartition => Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet
 *     Partition           => int32
 *     ErrorCode           => int16
 *     HighwaterMarkOffset => int64
 *     MessageSetSize      => int32
 * </pre>
 *
 * The message set is read as a byte array, because `MessageSetSize` is exactly the int32 length prefix of the
 * BYTEARRAY type; decoding those bytes into messages is the job of the record layer.
 *
 * `LastStableOffset`, `LogStartOffset` and `AbortedTransactions` belong to the transactional protocol of 0.11 and do
 * not exist in any version a 0.10.2.2 broker serves, in which the partition entry is the same in v0 to v3.
 *
 * @see docs/protocol/0.11.0.md, sections "Fetch API (key 1, v0 to v3)" and "MessageSet and Message"
 */
class FetchResponsePartition implements BinarySchemaInterface
{
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
     * Raw bytes of the returned message set, exactly as they lie in the log.
     *
     * The broker is allowed to cut the last message of the set short, therefore these bytes are not necessarily a
     * sequence of complete messages.
     */
    public ?string $messageSet = null;

    /**
     * Lazily decoded message set of this partition
     */
    private ?MessageSet $decodedMessageSet = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'           => BinarySchema::TYPE_INT32,
            'errorCode'           => BinarySchema::TYPE_INT16,
            'highWaterMarkOffset' => BinarySchema::TYPE_INT64,
            'messageSet'          => BinarySchema::TYPE_BYTEARRAY,
        ];
    }

    /**
     * Decodes the raw bytes of this partition into a message set, dropping a partial trailing message.
     *
     * The bytes are decoded once and the result is kept, because a fetch loop asks for the messages of a partition
     * and for their offsets separately.
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
     * Checks whether the returned bytes begin with at least one complete message
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
