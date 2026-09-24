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

namespace Protocol\Kafka\Tests\Unit\Consumer\Internals;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Consumer\Internals\ShareSessionHandler;

/**
 * The share session of a share consumer on one leader (KIP-932): the epoch of the next request and what it names.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 */
#[CoversClass(ShareSessionHandler::class)]
final class ShareSessionHandlerTest extends TestCase
{
    private const string ORDERS = "\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01";

    private const string PAYMENTS = "\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02";

    public function testTheEpochZeroOpensTheSessionWithEveryPartition(): void
    {
        $handler = new ShareSessionHandler(1);
        self::assertTrue($handler->isNewSession());

        [$added, $forgotten] = $handler->prepareFetch([self::PAYMENTS => [1, 0], self::ORDERS => [2]]);

        self::assertSame(0, $handler->nextEpoch());
        self::assertSame([self::ORDERS => [2], self::PAYMENTS => [0, 1]], $added);
        self::assertSame([], $forgotten);
    }

    public function testALaterRequestNamesOnlyWhatCameAndWent(): void
    {
        $handler = new ShareSessionHandler(1);
        $handler->prepareFetch([self::ORDERS => [0, 1]]);
        $handler->handleResponse();

        self::assertSame([[], []], $handler->prepareFetch([self::ORDERS => [1, 0]]), 'unchanged: nothing named');
        $handler->handleResponse();
        [$added, $forgotten] = $handler->prepareFetch([self::ORDERS => [1], self::PAYMENTS => [0]]);

        self::assertSame(2, $handler->nextEpoch());
        self::assertSame([self::PAYMENTS => [0]], $added);
        self::assertSame([self::ORDERS => [0]], $forgotten);
        self::assertSame([self::ORDERS => [1], self::PAYMENTS => [0]], $handler->sessionPartitions());
    }

    public function testAResetStartsOverWithTheEpochZeroAndEveryPartition(): void
    {
        $handler = new ShareSessionHandler(1);
        $handler->prepareFetch([self::ORDERS => [0]]);
        $handler->handleResponse();
        $handler->handleResponse();

        $handler->reset();

        self::assertTrue($handler->isNewSession());
        self::assertSame([], $handler->sessionPartitions());
        self::assertSame([[self::ORDERS => [0]], []], $handler->prepareFetch([self::ORDERS => [0]]));
    }
}
