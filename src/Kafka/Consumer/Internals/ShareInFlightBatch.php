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

namespace Protocol\Kafka\Consumer\Internals;

use LogicException;
use Protocol\Kafka\Consumer\AcknowledgeType;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;

/**
 * The records of one topic-partition a share consumer holds, and what the application said about them (KIP-932)
 *
 * `ShareInFlightBatch` of the Java client @ 4.3.1, together with the `Acknowledgements` it collects. A record is
 * **in flight** from the poll() that returned it until it is acknowledged; the acknowledgement is kept here, keyed by
 * offset, until the next request to the leader that acquired the record carries it - a ShareFetch of poll(), or a
 * ShareAcknowledge of a commit or of the close - and {@see self::takeAcknowledgements()} hands it over.
 *
 * Three more states come from the protocol rather than from the application:
 *
 * - an acquired offset that holds **no record** (a control record of a transaction, an offset compaction removed) is
 *   acknowledged with the type 0 **Gap** by the consumer itself ({@see self::addGap()}), as `ShareCompletedFetch`
 *   does it, and never shown to the application;
 * - a record acknowledged with **RENEW** (KIP-1222) leaves the flight while its renewal is on the wire
 *   ({@see self::takeAcknowledgements()}), is held once the node answered the renewal
 *   ({@see self::completeRenewals()}) and comes back into the flight when poll() returns it again
 *   ({@see self::takeRenewedRecords()}), which is the renewing and renewed state of the Java batch;
 * - a record a deserializer failed on is released by the consumer itself ({@see self::addFailedRecord()}) and
 *   stays acknowledgeable by its offset, so that the application may reject it instead, as the exception offsets of
 *   the Java batch are;
 * - the node that acquired the records is remembered, because an acknowledgement has to reach the share session it
 *   was acquired in.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 */
final class ShareInFlightBatch
{
    /**
     * Records the application has been given and not acknowledged yet, by offset
     *
     * @var array<int, ConsumerRecord>
     */
    private array $inFlightRecords = [];

    /**
     * Acknowledgements not sent yet, as offset => the wire id of the type (0 for a gap)
     *
     * @var array<int, int>
     */
    private array $acknowledgements = [];

    /**
     * Records whose renewal is on the wire, by offset
     *
     * @var array<int, ConsumerRecord>
     */
    private array $renewingRecords = [];

    /**
     * Records the node renewed, which the next poll() returns again, by offset
     *
     * @var array<int, ConsumerRecord>
     */
    private array $renewedRecords = [];

    /**
     * Offsets of acquired records a deserializer failed on, which were released and may be acknowledged by offset
     *
     * @var array<int, true>
     */
    private array $failedOffsets = [];

    /**
     * @param int    $nodeId    Leader that acquired the records, whose share session the acknowledgements go to
     * @param string $topic     Name of the topic
     * @param string $topicId   The 16 raw bytes of the id of the topic
     * @param int    $partition Partition of the topic
     */
    public function __construct(
        public readonly int $nodeId,
        public readonly string $topic,
        public readonly string $topicId,
        public readonly int $partition
    ) {}

    /**
     * Adds a record the node acquired for this member and poll() returns
     */
    public function addRecord(ConsumerRecord $record): void
    {
        $this->inFlightRecords[(int) $record->offset] = $record;
    }

    /**
     * Acknowledges an acquired offset that holds no record with the type 0 `Gap`
     */
    public function addGap(int $offset): void
    {
        $this->acknowledgements[$offset] = ShareAcknowledgementBatch::GAP;
    }

    /**
     * Releases an acquired record a deserializer failed on, which poll() cannot return
     */
    public function addFailedRecord(int $offset): void
    {
        $this->acknowledgements[$offset] = AcknowledgeType::RELEASE->value;
        $this->failedOffsets[$offset]    = true;
    }

    /**
     * Acknowledges one record in flight, replacing an acknowledgement of it that was not sent yet
     *
     * @throws LogicException If the record is not in flight: it was not returned by the last poll(), or it was
     *                        acknowledged and sent already
     */
    public function acknowledge(int $offset, AcknowledgeType $type): void
    {
        if (!isset($this->inFlightRecords[$offset])) {
            throw new LogicException('The record cannot be acknowledged.');
        }
        $this->acknowledgements[$offset] = $type->value;
    }

    /**
     * Tells whether an offset may be acknowledged by its number: a record in flight, or one a deserializer failed on
     */
    public function isAcknowledgeable(int $offset): bool
    {
        return isset($this->inFlightRecords[$offset]) || (isset($this->failedOffsets[$offset], $this->acknowledgements[$offset]));
    }

    /**
     * Acknowledges an offset by its number, a record in flight or one a deserializer failed on
     *
     * @throws LogicException If the offset is neither
     */
    public function acknowledgeOffset(int $offset, AcknowledgeType $type): void
    {
        if (!$this->isAcknowledgeable($offset)) {
            throw new LogicException('The record cannot be acknowledged.');
        }
        $this->acknowledgements[$offset] = $type->value;
    }

    /**
     * Acknowledges every record in flight that has no acknowledgement yet, which is what the implicit mode does
     */
    public function acknowledgeAll(AcknowledgeType $type): void
    {
        foreach (array_keys($this->inFlightRecords) as $offset) {
            $this->acknowledgements[$offset] ??= $type->value;
        }
    }

    /**
     * Tells whether every record in flight has an acknowledgement, which the explicit mode requires before a poll()
     */
    public function allInFlightAcknowledged(): bool
    {
        return array_diff_key($this->inFlightRecords, $this->acknowledgements) === [];
    }

    /**
     * Tells whether the given offset is a record in flight
     */
    public function isInFlight(int $offset): bool
    {
        return isset($this->inFlightRecords[$offset]);
    }

    /**
     * Tells whether this batch has nothing left: no record in flight, nothing to send and no renewal on the wire
     */
    public function isEmpty(): bool
    {
        return $this->inFlightRecords === []
            && $this->acknowledgements === []
            && $this->renewingRecords === []
            && $this->renewedRecords === [];
    }

    /**
     * Tells whether an acknowledgement waits to be sent
     */
    public function hasAcknowledgements(): bool
    {
        return $this->acknowledgements !== [];
    }

    /**
     * Returns the records in flight, in offset order
     *
     * @return list<ConsumerRecord>
     */
    public function inFlightRecords(): array
    {
        ksort($this->inFlightRecords);

        return array_values($this->inFlightRecords);
    }

    /**
     * Hands the acknowledgements over to a request: the acknowledged records leave the flight, the renewed ones wait
     * for the answer of the node in {@see self::completeRenewals()}
     *
     * @return array<int, int> Offset => the wire id of the type, in offset order
     */
    public function takeAcknowledgements(): array
    {
        $acknowledgements = $this->acknowledgements;
        ksort($acknowledgements);
        foreach ($acknowledgements as $offset => $type) {
            if ($type === AcknowledgeType::RENEW->value && isset($this->inFlightRecords[$offset])) {
                $this->renewingRecords[$offset] = $this->inFlightRecords[$offset];
            }
            unset($this->inFlightRecords[$offset]);
        }
        $this->acknowledgements = [];
        $this->failedOffsets    = [];

        return $acknowledgements;
    }

    /**
     * Ends the renewals on the wire: the records are held for the next poll() when the node renewed them, and they
     * are gone when it refused them - their delivery ends, as it does in the Java client
     */
    public function completeRenewals(bool $renewed): void
    {
        if ($renewed) {
            $this->renewedRecords = $this->renewingRecords + $this->renewedRecords;
        }
        $this->renewingRecords = [];
    }

    /**
     * Moves the renewed records back into the flight, for poll() to return them again
     *
     * @return list<ConsumerRecord> The records that came back, in offset order
     */
    public function takeRenewedRecords(): array
    {
        $records              = $this->renewedRecords;
        $this->renewedRecords = [];
        foreach ($records as $offset => $record) {
            $this->inFlightRecords[$offset] = $record;
        }
        ksort($records);

        return array_values($records);
    }

    /**
     * Tells whether a renewal of this batch is on the wire or waits for the next poll()
     */
    public function hasRenewals(): bool
    {
        return $this->renewingRecords !== [] || $this->renewedRecords !== [];
    }

    /**
     * Encodes acknowledgements as the acknowledgement batches of the wire
     *
     * Consecutive offsets of the same type become one batch with a single type - the form the node reads as "this
     * type for the whole range" -, so the implicit acceptance of a whole poll() is one batch per run of offsets. A
     * gap in the offsets, or a change of type, starts a new batch. `Acknowledgements.getAcknowledgementBatches()` of
     * the Java client @ 4.3.1 packs short runs of mixed types into one batch with a type per offset instead; the node
     * reads both forms the same way.
     *
     * @param array<int, int> $acknowledgements Offset => the wire id of the type
     *
     * @return list<ShareAcknowledgementBatch>
     */
    public static function acknowledgementBatches(array $acknowledgements): array
    {
        ksort($acknowledgements);

        $batches = [];
        $current = null;
        foreach ($acknowledgements as $offset => $type) {
            if ($current !== null && $offset === $current->lastOffset + 1 && $current->acknowledgeTypes === [$type]) {
                $current->lastOffset = $offset;
                continue;
            }
            $current   = ShareAcknowledgementBatch::of($offset, $offset, $type);
            $batches[] = $current;
        }

        return $batches;
    }

    /**
     * Tells whether one of the acknowledgements is a Renew, which the request carrying them has to announce
     *
     * @param array<int, int> $acknowledgements Offset => the wire id of the type
     */
    public static function hasRenewAcknowledgement(array $acknowledgements): bool
    {
        return in_array(AcknowledgeType::RENEW->value, $acknowledgements, true);
    }
}
