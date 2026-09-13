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

namespace Protocol\Kafka\Tests\Unit\Common\Record;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Record\ControlRecordKey;
use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\Common\Record\EndTransactionMarker;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;

/**
 * The marker that a transaction coordinator writes into every partition of a transaction it ends.
 *
 * The reference bytes are the ones of the vector `messageformat.v2.none.control`, the batch that the coordinator of
 * the 0.11.0.3 container appended when the transaction of `messageformat.v2.none.transactional` was committed: the
 * key `00 00 00 01` and the value `00 00 00 00 00 00`.
 *
 * @see docs/protocol/2.8.md, section "RecordBatch (message format v2)"
 */
#[CoversClass(EndTransactionMarker::class)]
#[CoversClass(ControlRecordType::class)]
#[CoversClass(ControlRecordKey::class)]
final class EndTransactionMarkerTest extends TestCase
{
    /**
     * Key of the control record that the broker wrote: the schema version 0 and the type 1, COMMIT
     */
    private const string BROKER_KEY = '00000001';

    /**
     * Value of the control record that the broker wrote: the schema version 0 and the coordinator epoch 0
     */
    private const string BROKER_VALUE = '000000000000';

    public function testTheKeyOfTheBrokerIsACommitMarker(): void
    {
        self::assertSame(ControlRecordType::COMMIT, ControlRecordType::parse((string) hex2bin(self::BROKER_KEY)));
    }

    public function testTheMarkerOfTheBrokerIsDecodedIntoItsFields(): void
    {
        $record = new Record((string) hex2bin(self::BROKER_VALUE), (string) hex2bin(self::BROKER_KEY));

        $marker = EndTransactionMarker::fromRecord($record);

        self::assertSame(ControlRecordType::COMMIT, $marker->controlType);
        self::assertSame(0, $marker->coordinatorEpoch);
        self::assertSame(EndTransactionMarker::CURRENT_VERSION, $marker->version);
    }

    public function testAMarkerIsSerializedTheWayTheBrokerSerializesIt(): void
    {
        $marker = new EndTransactionMarker(ControlRecordType::COMMIT, 0);

        self::assertSame(self::BROKER_KEY, bin2hex($marker->serializeKey()));
        self::assertSame(self::BROKER_VALUE, bin2hex($marker->serializeValue()));
        self::assertSame(ControlRecordType::CURRENT_KEY_SIZE, strlen($marker->serializeKey()));
        self::assertSame(EndTransactionMarker::CURRENT_VALUE_SIZE, strlen($marker->serializeValue()));
    }

    public function testAnAbortMarkerCarriesTheTypeZero(): void
    {
        $marker = new EndTransactionMarker(ControlRecordType::ABORT, 4);

        self::assertSame('00000000', bin2hex($marker->serializeKey()));
        self::assertSame('000000000004', bin2hex($marker->serializeValue()));
        self::assertSame(ControlRecordType::ABORT, EndTransactionMarker::fromRecord($marker->toRecord())->controlType);
        self::assertSame(4, EndTransactionMarker::fromRecord($marker->toRecord())->coordinatorEpoch);
    }

    public function testAMarkerSurvivesTheBatchThatCarriesIt(): void
    {
        $marker = new EndTransactionMarker(ControlRecordType::ABORT, 11);
        $batch  = RecordBatch::fromEndTransactionMarker($marker, 4711, 3, 1600000000000);

        $read = EndTransactionMarker::fromRecord(RecordBatch::fromBuffer($batch->toBuffer())->getRecords()[0]);

        self::assertSame(ControlRecordType::ABORT, $read->controlType);
        self::assertSame(11, $read->coordinatorEpoch);
    }

    public function testAnUnknownControlTypeIsIgnoredRatherThanRefused(): void
    {
        self::assertSame(ControlRecordType::UNKNOWN, ControlRecordType::parse((string) hex2bin('00000009')));
    }

    public function testAControlKeyThatIsTooShortIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        ControlRecordType::parse((string) hex2bin('000000'));
    }

    public function testAControlKeyOfANegativeVersionIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        ControlRecordType::parse((string) hex2bin('ffff0001'));
    }

    public function testAControlRecordWithoutAKeyIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        ControlRecordType::parse(null);
    }

    public function testAMarkerValueThatIsTooShortIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        EndTransactionMarker::fromRecord(new Record((string) hex2bin('0000'), (string) hex2bin(self::BROKER_KEY)));
    }

    public function testAControlRecordOfAnUnknownTypeIsNotAMarker(): void
    {
        $this->expectException(CorruptMessageException::class);
        EndTransactionMarker::fromRecord(
            new Record((string) hex2bin(self::BROKER_VALUE), (string) hex2bin('00000009'))
        );
    }

    public function testAnUnknownControlTypeCanNotBeSerialized(): void
    {
        $this->expectException(\UnexpectedValueException::class);
        ControlRecordType::key(ControlRecordType::UNKNOWN);
    }

    public function testAMarkerIsOnlyTheEndOfATransaction(): void
    {
        $this->expectException(\UnexpectedValueException::class);
        new EndTransactionMarker(ControlRecordType::UNKNOWN, 0);
    }

    public function testTheTypesAreNamedTheWayTheBrokerNamesThem(): void
    {
        self::assertSame('ABORT', ControlRecordType::name(ControlRecordType::ABORT));
        self::assertSame('COMMIT', ControlRecordType::name(ControlRecordType::COMMIT));
        self::assertSame('UNKNOWN', ControlRecordType::name(ControlRecordType::UNKNOWN));
        self::assertSame('Unknown control record type 9', ControlRecordType::name(9));
        self::assertTrue(ControlRecordType::isValid(ControlRecordType::COMMIT));
        self::assertFalse(ControlRecordType::isValid(9));
    }
}
