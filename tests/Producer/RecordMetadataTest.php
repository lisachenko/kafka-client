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

namespace Protocol\Kafka\Tests\Producer;

use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Producer\RecordMetadata;

final class RecordMetadataTest extends TestCase
{
    public function testExposesTopicPartitionOffsetAndTimestamp(): void
    {
        $metadata = new RecordMetadata('orders', 2, 1234, 1_700_000_000);

        self::assertSame('orders', $metadata->topic);
        self::assertSame(2, $metadata->partition);
        self::assertSame(1234, $metadata->offset);
        self::assertSame(1_700_000_000, $metadata->timestamp);
    }

    public function testTimestampIsOptional(): void
    {
        $metadata = new RecordMetadata('orders', 2, 1234);

        self::assertNull($metadata->timestamp);
    }

    public function testToStringFormat(): void
    {
        self::assertSame('orders-2@1234', (string) new RecordMetadata('orders', 2, 1234));
    }
}
