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

namespace Protocol\Kafka\Tests\Unit\Producer;

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

    public function testTheThrottleTimeOfTheBatchIsReported(): void
    {
        // Produce v1 reports how long the broker delayed the answer because of a `producer_byte_rate` quota
        $metadata = new RecordMetadata('orders', 2, 1234, null, 793);

        self::assertSame(793, $metadata->throttleTimeMs);
    }

    public function testTheThrottleTimeIsZeroWithoutAQuota(): void
    {
        self::assertSame(0, new RecordMetadata('orders', 2, 1234)->throttleTimeMs);
    }

    public function testTheTimestampIsTheOneTheLogHolds(): void
    {
        // Either the CreateTime the producer stamped on the first record of the batch, or the LogAppendTime the
        // broker answered a Produce v2 request with for a `message.timestamp.type=LogAppendTime` topic
        $createTime = new RecordMetadata('orders', 0, 7, 1489324800000);
        $appendTime = new RecordMetadata('orders', 0, 7, 1788933842981);

        self::assertSame(1489324800000, $createTime->timestamp);
        self::assertSame(1788933842981, $appendTime->timestamp);
    }

    public function testToStringFormat(): void
    {
        self::assertSame('orders-2@1234', (string) new RecordMetadata('orders', 2, 1234));
    }
}
