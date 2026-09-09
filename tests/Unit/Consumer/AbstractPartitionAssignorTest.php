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

namespace Protocol\Kafka\Tests\Unit\Consumer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Consumer\AbstractPartitionAssignor;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\PartitionAssignorInterface;
use Protocol\Kafka\Consumer\RangeAssignor;
use Protocol\Kafka\Consumer\RoundRobinAssignor;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\Tests\Unit\Consumer\Fixture\StickyLikeAssignor;

/**
 * Covers what every assignor shares: the resolution of the `partition.assignment.strategy` option and the
 * translation between the plain lists an implementation works on and the structures of the protocol.
 */
#[CoversClass(AbstractPartitionAssignor::class)]
final class AbstractPartitionAssignorTest extends TestCase
{
    public function testResolvesTheWireNamesOfTheBuiltInAssignors(): void
    {
        self::assertInstanceOf(RangeAssignor::class, AbstractPartitionAssignor::fromStrategy('range'));
        self::assertInstanceOf(RoundRobinAssignor::class, AbstractPartitionAssignor::fromStrategy('roundrobin'));
    }

    public function testResolvesTheDefaultOfTheConsumerConfiguration(): void
    {
        $strategy = ConsumerConfig::getDefaultConfiguration()[ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY];

        self::assertSame('range', $strategy);
        self::assertSame('range', AbstractPartitionAssignor::fromStrategy($strategy)->name());
    }

    public function testResolvesTheClassNameOfACustomAssignor(): void
    {
        $assignor = AbstractPartitionAssignor::fromStrategy(StickyLikeAssignor::class);

        self::assertInstanceOf(StickyLikeAssignor::class, $assignor);
        self::assertSame('sticky-like', $assignor->name());
    }

    public function testRejectsAStrategyThatIsNeitherBuiltInNorAnAssignorClass(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Unknown partition assignment strategy sticky');

        AbstractPartitionAssignor::fromStrategy('sticky');
    }

    public function testRejectsAClassThatDoesNotImplementTheInterface(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        AbstractPartitionAssignor::fromStrategy(self::class);
    }

    public function testACustomAssignorCanForwardItsOwnUserData(): void
    {
        $assignor     = new StickyLikeAssignor();
        $subscription = $assignor->subscription(['topic']);

        self::assertSame(['topic'], $subscription->topics);
        self::assertSame('previous-generation', $subscription->userData);
        self::assertInstanceOf(PartitionAssignorInterface::class, $assignor);
    }

    public function testACustomAssignorDecidesTheAssignmentOnItsOwnUserData(): void
    {
        $assignor   = new StickyLikeAssignor();
        $assignment = $assignor->assign(['topic' => 2], [
            'consumer1' => new Subscription(['topic'], userData: 'keep-1'),
            'consumer2' => new Subscription(['topic'], userData: 'keep-0'),
        ]);

        self::assertSame(['topic' => [1]], $assignment['consumer1']->partitions());
        self::assertSame(['topic' => [0]], $assignment['consumer2']->partitions());
    }

    public function testTheBuiltInAssignorsKeepNoStateAndSendNoUserData(): void
    {
        // `AbstractPartitionAssignor` of the Java client wraps its result without any user data, and its
        // `subscription()` sends the empty byte array that `PartitionAssignor.Subscription` defaults to
        foreach ([new RangeAssignor(), new RoundRobinAssignor()] as $assignor) {
            self::assertSame('', $assignor->subscription(['topic'])->userData);

            $assignment = $assignor->assign(['topic' => 1], ['consumer' => new Subscription(['topic'])]);
            self::assertSame('', $assignment['consumer']->userData);
            self::assertSame(Subscription::VERSION, $assignment['consumer']->version);
        }
    }
}
