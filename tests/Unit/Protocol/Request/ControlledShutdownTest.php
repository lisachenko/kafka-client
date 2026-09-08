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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ControlledShutdownResponsePartition;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\ControlledShutdownResponse;

/**
 * Byte-exact tests of the ControlledShutdown API v0.
 *
 * <pre>
 *   ControlledShutdownRequest  => BrokerId int32
 *   ControlledShutdownResponse => ErrorCode int16 [TopicName string Partition int32]
 * </pre>
 *
 * @see docs/protocol/0.8.2.md, section "ControlledShutdown API (key 7, v0)"
 */
#[CoversClass(ControlledShutdownRequest::class)]
#[CoversClass(ControlledShutdownResponse::class)]
#[CoversClass(ControlledShutdownResponsePartition::class)]
final class ControlledShutdownTest extends TestCase
{
    public function testRequestHeaderCarriesNoClientId(): void
    {
        // Size = 12: ApiKey 7, ApiVersion 0, CorrelationId 20, BrokerId 4242 - and no ClientId string in between
        $request = new ControlledShutdownRequest(4242, 20);

        self::assertSame(
            '0000000c' . '0007' . '0000' . '00000014' . '00001092',
            bin2hex((string) $request)
        );
        self::assertSame(12, $request->getMessageSize());
        self::assertSame(ApiKeys::CONTROLLED_SHUTDOWN, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(4242, $request->getBrokerId());
    }

    public function testRequestSchemeDropsTheClientIdOfTheCommonHeader(): void
    {
        // Reading a client id string as the broker id is exactly the bug this guards against
        self::assertArrayNotHasKey('clientId', ControlledShutdownRequest::getScheme());
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'brokerId'],
            array_keys(ControlledShutdownRequest::getScheme())
        );
    }

    public function testResponseWithoutRemainingPartitionsIsDecoded(): void
    {
        // The answer of a 0.8.2.2 broker for a broker id the controller does not know: error -1, no partitions
        $response = ControlledShutdownResponse::unpack(
            new StringStream((string) hex2bin('0000000a' . '00000014' . 'ffff' . '00000000'))
        );

        self::assertSame(20, $response->getCorrelationId());
        self::assertSame(-1, $response->errorCode);
        self::assertSame([], $response->remainingTopicPartitions);
    }

    public function testResponseKeepsEveryRemainingTopicPartitionOfTheFlatList(): void
    {
        //   ErrorCode 0, then ("orders", 0), ("orders", 2) - the same topic appears twice, the list is not grouped
        $frame = '00000022' . '00000001' . '0000' . '00000002'
            . '0006' . '6f7264657273' . '00000000'
            . '0006' . '6f7264657273' . '00000002';

        $response = ControlledShutdownResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(0, $response->errorCode);
        self::assertCount(2, $response->remainingTopicPartitions);
        self::assertSame('orders', $response->remainingTopicPartitions[0]->topic);
        self::assertSame(0, $response->remainingTopicPartitions[0]->partition);
        self::assertSame('orders', $response->remainingTopicPartitions[1]->topic);
        self::assertSame(2, $response->remainingTopicPartitions[1]->partition);

        self::assertSame($frame, bin2hex((string) $response), 'the response has to survive a round trip');
    }
}
