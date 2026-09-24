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
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Record\ControlRecordKey;
use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * The key of a control record against `ControlRecordTypeSchema.json`, the data schema Kafka 4.3 gave it.
 *
 * Up to Kafka 4.2 the key was a hand-written `Schema` of `ControlRecordType.java`, `version` and `type`, both int16.
 * Kafka 4.3.0 replaced it with a generated message - `ControlRecordTypeSchema.json` @ 4.3.1: `"type": "data"`,
 * `"validVersions": "0"`, `"flexibleVersions": "none"` and the one field `Type` (int16) - written with
 * `MessageUtil.toVersionPrefixedByteBuffer`, which puts the int16 version of the schema in front of it. The bytes
 * did not change: four of them, the version 0 and the type. `ControlRecordType.parseTypeId` @ 4.3.1 (moved to
 * `org.apache.kafka.common.record.internal`) refuses a key shorter than four bytes and a version below 0, reads a
 * version above 0 as the version 0 and ignores what follows the type, and `fromTypeId` answers `UNKNOWN` for every
 * type it does not name. This class pins the same four rules on {@see ControlRecordType::parse()}, and the two keys
 * the 4.3.1 node wrote for a committed and an aborted transaction.
 *
 * @see docs/protocol/4.3.md, section "Control batches"
 */
#[CoversClass(ControlRecordType::class)]
#[CoversClass(ControlRecordKey::class)]
final class ControlRecordTypeTest extends TestCase
{
    /**
     * The key of the COMMIT marker the 4.3.1 node wrote behind a committed transaction (`kafka-dump-log.sh`:
     * `keySize: 4 ... endTxnMarker: COMMIT coordinatorEpoch: 0`)
     */
    private const string NODE_COMMIT_KEY = '00000001';

    /**
     * The key of the ABORT marker the 4.3.1 node wrote behind an aborted transaction
     */
    private const string NODE_ABORT_KEY = '00000000';

    public function testTheKeysOfTheNodeAreTheVersionPrefixedSchema(): void
    {
        self::assertSame(ControlRecordType::COMMIT, ControlRecordType::parse((string) hex2bin(self::NODE_COMMIT_KEY)));
        self::assertSame(ControlRecordType::ABORT, ControlRecordType::parse((string) hex2bin(self::NODE_ABORT_KEY)));
        self::assertSame(self::NODE_COMMIT_KEY, bin2hex(ControlRecordType::key(ControlRecordType::COMMIT)));
        self::assertSame(self::NODE_ABORT_KEY, bin2hex(ControlRecordType::key(ControlRecordType::ABORT)));
    }

    /**
     * `ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION` is 0, and its one field is the int16 behind the prefix
     */
    public function testTheKeyIsTheInt16VersionAndTheInt16TypeOfTheSchema(): void
    {
        self::assertSame(
            ['version' => BinarySchema::TYPE_INT16, 'type' => BinarySchema::TYPE_INT16],
            ControlRecordKey::getScheme()
        );
        self::assertSame(0, ControlRecordType::CURRENT_KEY_VERSION, 'the "validVersions": "0" of the schema');
        self::assertSame(4, ControlRecordType::CURRENT_KEY_SIZE, 'the CONTROL_RECORD_KEY_SIZE of Kafka 4.3.1');

        $stream = new StringStream();
        BinarySchema::writeObjectToStream(new ControlRecordKey(0, ControlRecordType::COMMIT), $stream);
        self::assertSame(self::NODE_COMMIT_KEY, bin2hex($stream->getBuffer()));
    }

    /**
     * A later version is read as the version 0: the type stays the int16 behind the version, the rest is ignored
     *
     * `ControlRecordTypeTest.testParseUnknownVersion` @ 4.3.1 sends the version 5, the type ABORT and an int32 of a
     * field a later release might add, and expects ABORT
     */
    public function testAKeyOfALaterVersionIsReadAsTheVersionZero(): void
    {
        $key = pack('n', 5) . pack('n', ControlRecordType::ABORT) . pack('N', 23432);

        self::assertSame(ControlRecordType::ABORT, ControlRecordType::parse($key));
        self::assertSame(ControlRecordType::COMMIT, ControlRecordType::parse(pack('n', 1) . pack('n', 1)));
    }

    /**
     * The KRaft markers of `ControlRecordType` @ 4.3.1 - LEADER_CHANGE (2), SNAPSHOT_HEADER (3), SNAPSHOT_FOOTER (4),
     * KRAFT_VERSION (5) and KRAFT_VOTERS (6) - are written into the metadata log, which a client never fetches; a
     * client that meets one ignores it, as it ignores every type it does not know
     *
     * @return array<string, array{int}>
     */
    public static function typeUnknownToAClientProvider(): array
    {
        return [
            'LEADER_CHANGE'   => [2],
            'SNAPSHOT_HEADER' => [3],
            'SNAPSHOT_FOOTER' => [4],
            'KRAFT_VERSION'   => [5],
            'KRAFT_VOTERS'    => [6],
            'type 337'        => [337],
            'type -2'         => [-2],
        ];
    }

    #[DataProvider('typeUnknownToAClientProvider')]
    public function testATypeThisClientDoesNotNameIsUnknown(int $type): void
    {
        $key = pack('n', 0) . pack('n', $type & 0xffff);

        self::assertSame(ControlRecordType::UNKNOWN, ControlRecordType::parse($key));
    }

    public function testAKeyShorterThanFourBytesIsCorrupt(): void
    {
        $this->expectException(CorruptMessageException::class);
        ControlRecordType::parse(pack('n', 0));
    }

    public function testAKeyOfANegativeVersionIsCorrupt(): void
    {
        $this->expectException(CorruptMessageException::class);
        ControlRecordType::parse(pack('n', 0xffff) . pack('n', ControlRecordType::COMMIT));
    }
}
