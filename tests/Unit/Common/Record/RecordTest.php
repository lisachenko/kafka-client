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
use Protocol\Kafka\Common\Record\Record;

/**
 * The user-facing record, the value object that the producer and the consumer deal in
 */
#[CoversClass(Record::class)]
final class RecordTest extends TestCase
{
    public function testARecordIsAValueWithAnOptionalKey(): void
    {
        $record = new Record('bar', 'foo');

        self::assertSame('bar', $record->value);
        self::assertSame('foo', $record->key);
        self::assertSame(0, $record->attributes);
        self::assertNull($record->offset, 'a record that was not read from a broker has no offset');
    }

    public function testTheValueIsEnoughToBuildARecord(): void
    {
        $record = new Record('bar');

        self::assertSame('bar', $record->value);
        self::assertNull($record->key);
    }

    public function testFactoriesBuildTheSameRecordsAsTheConstructor(): void
    {
        self::assertEquals(new Record('bar'), Record::fromValue('bar'));
        self::assertEquals(new Record('bar', 'foo'), Record::fromKeyValue('foo', 'bar'));
        self::assertEquals(new Record('bar', 'foo', 1), Record::fromKeyValue('foo', 'bar', 1));
    }

    public function testARecordReadFromABrokerCarriesItsOffset(): void
    {
        $record = new Record('bar', 'foo', 0, 42);

        self::assertSame(42, $record->offset);
    }
}
