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

namespace Protocol\Kafka\Tests\Common;

use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\TopicPartition;

final class TopicPartitionTest extends TestCase
{
    public function testEqualsComparesTopicAndPartition(): void
    {
        $a = new TopicPartition('orders', 3);
        $b = new TopicPartition('orders', 3);
        $c = new TopicPartition('orders', 4);
        $d = new TopicPartition('payments', 3);

        self::assertTrue($a->equals($b));
        self::assertFalse($a->equals($c));
        self::assertFalse($a->equals($d));
    }

    public function testToStringFormat(): void
    {
        self::assertSame('orders-3', (string) new TopicPartition('orders', 3));
    }
}
