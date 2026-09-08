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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Stringable;

/**
 * One partition of a Fetch response v0
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
 * not exist in v0.
 *
 * @see docs/protocol/0.8.2.md, sections "Fetch API (key 1, v0)" and "MessageSet and Message"
 */
class FetchResponsePartition implements BinarySchemaInterface
{
    /**
     * Decoder of the raw message set bytes.
     *
     * Resolved by name on purpose: the record layer of the 0.8 line is developed in parallel with the protocol
     * classes, and this class only depends on its `fromBuffer()` entry point.
     *
     * @see \Protocol\Kafka\Common\Record\MessageSet
     */
    private const string MESSAGE_SET_CLASS = 'Protocol\Kafka\Common\Record\MessageSet';

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
    private ?Stringable $decodedMessageSet = null;

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
     * @return Stringable The `Protocol\Kafka\Common\Record\MessageSet` of the record layer
     */
    public function getMessageSet(): Stringable
    {
        if ($this->decodedMessageSet === null) {
            $messageSetClass = self::MESSAGE_SET_CLASS;
            if (!class_exists($messageSetClass)) {
                throw new \LogicException(
                    "Class {$messageSetClass} is not available, the raw bytes are in the messageSet property"
                );
            }
            $this->decodedMessageSet = $messageSetClass::fromBuffer($this->messageSet ?? '');
        }

        return $this->decodedMessageSet;
    }

    /**
     * Tells whether a single message of this partition is bigger than the MaxBytes that were asked for.
     *
     * A 0.8.2.2 broker cuts the message set off at `MaxBytes` and does not guarantee any progress, unlike the later
     * protocol versions: when the message at `FetchOffset` is bigger than that limit, the partition comes back
     * without an error and with a message set that holds no complete message at all - either nothing or the first
     * bytes of that one message - while its high water mark shows that there is something to read. A consumer that
     * keeps fetching the same offset would spin forever, so it has to raise `max.partition.fetch.bytes` instead.
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
