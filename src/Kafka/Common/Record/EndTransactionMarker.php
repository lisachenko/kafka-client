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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The Value of the control record that ends a transaction: the marker that a coordinator writes on commit or abort.
 *
 * <pre>
 *   Value => Version CoordinatorEpoch
 *     Version          => int16 (0)
 *     CoordinatorEpoch => int32
 * </pre>
 *
 * Which of the two ends it is - {@see ControlRecordType::COMMIT} or {@see ControlRecordType::ABORT} - is not in the
 * value, it is in the {@see ControlRecordKey} of the same record; the value only adds the epoch of the transaction
 * coordinator that wrote the marker, which lets a partition leader refuse a marker from a coordinator that has
 * already been replaced (a zombie).
 *
 * A marker always sits alone in a control batch of exactly one record, whose `producerId` and `producerEpoch` are
 * the ones of the transaction it ends and whose `baseSequence` is -1: the marker is not part of the sequence of the
 * producer.
 *
 * @see docs/protocol/1.1.md, section "RecordBatch (message format v2)"
 * @see org/apache/kafka/common/record/EndTransactionMarker.java @ 0.11.0.3
 */
class EndTransactionMarker implements BinarySchemaInterface
{
    /**
     * Version of the marker value schema that Kafka 0.11 writes
     */
    public const int CURRENT_VERSION = 0;

    /**
     * Size of the value of a marker: the int16 version and the int32 coordinator epoch
     */
    public const int CURRENT_VALUE_SIZE = 6;

    /**
     * Version of the value schema, kept as a field because a reader has to accept a higher one
     */
    public int $version = self::CURRENT_VERSION;

    /**
     * Epoch of the transaction coordinator that wrote this marker
     */
    public int $coordinatorEpoch = 0;

    /**
     * Which end of the transaction this marker is, out of the Key of its control record
     */
    public int $controlType = ControlRecordType::COMMIT;

    /**
     * @param int $controlType      {@see ControlRecordType::COMMIT} or {@see ControlRecordType::ABORT}
     * @param int $coordinatorEpoch Epoch of the transaction coordinator that wrote the marker
     * @param int $version          Version of the value schema
     */
    public function __construct(
        int $controlType = ControlRecordType::COMMIT,
        int $coordinatorEpoch = 0,
        int $version = self::CURRENT_VERSION,
    ) {
        if ($controlType !== ControlRecordType::COMMIT && $controlType !== ControlRecordType::ABORT) {
            throw new \UnexpectedValueException(
                "Control record type {$controlType} is not an end of a transaction"
            );
        }

        $this->controlType      = $controlType;
        $this->coordinatorEpoch = $coordinatorEpoch;
        $this->version          = $version;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'version'          => BinarySchema::TYPE_INT16,
            'coordinatorEpoch' => BinarySchema::TYPE_INT32,
        ];
    }

    /**
     * Reads the marker out of the control record that carries it
     *
     * @throws CorruptMessageException when the record is not an end of transaction marker
     */
    public static function fromRecord(Record $record): self
    {
        $type = ControlRecordType::parse($record->key);
        if ($type !== ControlRecordType::COMMIT && $type !== ControlRecordType::ABORT) {
            throw new CorruptMessageException([
                'error' => 'A control record of another type than the end of a transaction was read as a marker',
                'type'  => $type,
            ]);
        }

        $value = $record->value ?? '';
        if (strlen($value) < self::CURRENT_VALUE_SIZE) {
            throw new CorruptMessageException([
                'error'     => 'The value of an end transaction marker is shorter than its schema',
                'valueSize' => strlen($value),
            ]);
        }

        /** @var self $marker */
        $marker = BinarySchema::readObjectFromStream(self::class, new StringStream($value), 'endTxnMarker');
        if ($marker->version < 0) {
            throw new CorruptMessageException([
                'error'   => 'The value of an end transaction marker announces a negative schema version',
                'version' => $marker->version,
            ]);
        }
        $marker->controlType = $type;

        return $marker;
    }

    /**
     * Serializes the Key of the control record that carries this marker
     */
    public function serializeKey(): string
    {
        return ControlRecordType::key($this->controlType);
    }

    /**
     * Serializes the Value of the control record that carries this marker
     */
    public function serializeValue(): string
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream($this, $stream);

        return $stream->getBuffer();
    }

    /**
     * Returns the control record of this marker, the single record of the control batch that carries it
     */
    public function toRecord(): Record
    {
        return new Record($this->serializeValue(), $this->serializeKey());
    }
}
