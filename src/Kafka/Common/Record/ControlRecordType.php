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

/**
 * The types of a control record, the markers that a transaction coordinator writes into a partition (KIP-98).
 *
 * A {@see RecordBatch} whose attributes have bit 5 set is a **control batch**: its records are not data, they are
 * markers of the transaction protocol, and a client never hands them to an application - `getRecords()` of
 * {@see MemoryRecords} drops them, and a `read_committed` consumer uses them to decide which of the batches around
 * them belong to a committed transaction. In Kafka 0.11 the only markers are the two ends of a transaction, and
 * their value is an {@see EndTransactionMarker}.
 *
 * The `Key` of such a record is a {@see ControlRecordKey}, a version and one of the types below; a type this client
 * does not know is {@see ControlRecordType::UNKNOWN} and is ignored rather than refused, which is what lets a later
 * broker add markers.
 *
 * @see docs/protocol/0.11.0.md, section "RecordBatch (message format v2)"
 * @see org/apache/kafka/common/record/ControlRecordType.java @ 0.11.0.3
 */
final class ControlRecordType
{
    /**
     * The transaction that the batches before this marker belong to was aborted
     */
    public const int ABORT = 0;

    /**
     * The transaction that the batches before this marker belong to was committed
     */
    public const int COMMIT = 1;

    /**
     * A control record of a type that this client does not know and therefore ignores
     */
    public const int UNKNOWN = -1;

    /**
     * Version of the control record key schema that Kafka 0.11 writes
     */
    public const int CURRENT_KEY_VERSION = 0;

    /**
     * Size of the key of a control record: the two int16 fields of {@see ControlRecordKey}
     */
    public const int CURRENT_KEY_SIZE = 4;

    /**
     * Names of the control record types, the way the broker and its tools spell them
     */
    private const array NAMES = [
        self::ABORT   => 'ABORT',
        self::COMMIT  => 'COMMIT',
        self::UNKNOWN => 'UNKNOWN',
    ];

    /**
     * This class is only a namespace for the control record type constants and is never instantiated
     */
    private function __construct() {}

    /**
     * Returns the type of a control record out of the bytes of its Key
     *
     * A key that is too short or announces a negative version is corrupt, the way `ControlRecordType.parseTypeId()`
     * sees it; a type that this client does not know answers {@see ControlRecordType::UNKNOWN}.
     *
     * @throws CorruptMessageException when the key is not a control record key at all
     */
    public static function parse(?string $key): int
    {
        if ($key === null || strlen($key) < self::CURRENT_KEY_SIZE) {
            throw new CorruptMessageException([
                'error'   => 'The key of a control record is shorter than the control record key schema',
                'keySize' => $key === null ? -1 : strlen($key),
            ]);
        }

        /** @var ControlRecordKey $parsed */
        $parsed = BinarySchema::readObjectFromStream(ControlRecordKey::class, new StringStream($key), 'controlKey');
        if ($parsed->version < 0) {
            throw new CorruptMessageException([
                'error'   => 'The key of a control record announces a negative schema version',
                'version' => $parsed->version,
            ]);
        }

        return self::isValid($parsed->type) ? $parsed->type : self::UNKNOWN;
    }

    /**
     * Serializes the Key of a control record of the given type
     */
    public static function key(int $type): string
    {
        if (!self::isValid($type) || $type === self::UNKNOWN) {
            throw new \UnexpectedValueException("Control record type {$type} can not be serialized");
        }

        $stream = new StringStream();
        BinarySchema::writeObjectToStream(new ControlRecordKey(self::CURRENT_KEY_VERSION, $type), $stream);

        return $stream->getBuffer();
    }

    /**
     * Tells whether the given value is one of the control record types of this protocol line
     */
    public static function isValid(int $type): bool
    {
        return isset(self::NAMES[$type]);
    }

    /**
     * Returns the name of a control record type, the way the broker spells it
     */
    public static function name(int $type): string
    {
        return self::NAMES[$type] ?? "Unknown control record type {$type}";
    }
}
