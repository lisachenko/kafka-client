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

namespace Protocol\Kafka\Tests\Common\Serialization;

use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Common\Serialization\StringSerializer;

final class StringSerializationTest extends TestCase
{
    public function testSerializerPassesStringsThrough(): void
    {
        self::assertSame('value', new StringSerializer()->serialize('topic', 'value'));
    }

    public function testSerializerStringifiesNonStrings(): void
    {
        self::assertSame('42', new StringSerializer()->serialize('topic', 42));
    }

    public function testDeserializerPassesStringsThrough(): void
    {
        self::assertSame('value', new StringDeserializer()->deserialize('topic', 'value'));
    }
}
