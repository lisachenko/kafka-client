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

namespace Protocol\Kafka\Tests\Common\Record;

use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;

final class HeaderTest extends TestCase
{
    public function testWriteThenReadObjectRoundTrips(): void
    {
        $header = new Header('content-type', 'application/json');

        $writeStream = new StringStream();
        BinarySchema::writeObjectToStream($header, $writeStream);

        $readStream = new StringStream($writeStream->getBuffer());
        $result = BinarySchema::readObjectFromStream(Header::class, $readStream);

        self::assertSame($header->key, $result->key);
        self::assertSame($header->value, $result->value);
    }
}
