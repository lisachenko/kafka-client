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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ControlledShutdownResponsePartition;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequestV0;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequestV1;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequestV2;
use Protocol\Kafka\Protocol\Request\ControlledShutdownResponse;
use Protocol\Kafka\Protocol\Request\ControlledShutdownResponseV2;

/**
 * Byte-exact tests of the ControlledShutdown API, versions 0 and 1.
 *
 * <pre>
 *   ControlledShutdownRequest  => BrokerId int32
 *   ControlledShutdownResponse => ErrorCode int16 [TopicName string Partition int32]
 * </pre>
 *
 * The versions differ in their header alone: version 1, added by Kafka 0.9, carries the client id of the common
 * request header, version 0 has no client id at all.
 *
 * @see docs/protocol/2.8.md, section "ControlledShutdown API (key 7, v0 to v3)"
 */
#[CoversClass(ControlledShutdownRequest::class)]
#[CoversClass(ControlledShutdownRequestV0::class)]
#[CoversClass(ControlledShutdownRequestV1::class)]
#[CoversClass(ControlledShutdownResponse::class)]
#[CoversClass(ControlledShutdownResponsePartition::class)]
#[CoversClass(ControlledShutdownRequestV2::class)]
#[CoversClass(ControlledShutdownResponseV2::class)]
final class ControlledShutdownTest extends TestCase
{
    /**
     * The version Kafka 2.2 added with KIP-380: the broker epoch behind the broker id
     */
    public function testVersion2RequestCarriesTheBrokerEpochOfKip380(): void
    {
        // Size = 30: ApiKey 7, ApiVersion 2, CorrelationId 21, ClientId "t2-probe", BrokerId 4242, BrokerEpoch -1
        $request = new ControlledShutdownRequestV2(4242, ControlledShutdownRequest::UNKNOWN_BROKER_EPOCH, 't2-probe', 21);

        self::assertSame(
            '0000001e' . '0007' . '0002' . '00000015' . '0008' . bin2hex('t2-probe') . '00001092'
            . 'ffffffffffffffff',
            bin2hex((string) $request)
        );
        self::assertSame(30, $request->getMessageSize());
        self::assertSame(ApiKeys::CONTROLLED_SHUTDOWN, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion());
        self::assertSame(4242, $request->getBrokerId());
        self::assertSame(-1, $request->getBrokerEpoch());
        self::assertSame(-1, ControlledShutdownRequest::UNKNOWN_BROKER_EPOCH);
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'brokerId', 'brokerEpoch'],
            array_keys(ControlledShutdownRequestV2::getScheme()),
            'the version 2 frame; the flexible version 3 of KIP-482 adds a tag buffer to the header and the body'
        );
    }

    /**
     * A real epoch is the int64 behind the broker id; the controller refuses a lower one than the cached one
     */
    public function testABrokerEpochIsTheTrailingInt64OfTheVersionTwoFrame(): void
    {
        $request = new ControlledShutdownRequestV2(4242, 7, 't2-probe', 21);

        self::assertStringEndsWith('00001092' . '0000000000000007', bin2hex((string) $request));
        self::assertSame(7, $request->getBrokerEpoch());
    }

    public function testVersion1RequestUsesTheCommonHeaderWithTheClientId(): void
    {
        // Size = 22: ApiKey 7, ApiVersion 1, CorrelationId 21, ClientId "t2-probe", BrokerId 4242, no epoch
        $request = new ControlledShutdownRequestV1(4242, ControlledShutdownRequest::UNKNOWN_BROKER_EPOCH, 't2-probe', 21);

        self::assertSame(
            '00000016' . '0007' . '0001' . '00000015' . '0008' . bin2hex('t2-probe') . '00001092',
            bin2hex((string) $request)
        );
        self::assertSame(22, $request->getMessageSize());
        self::assertSame(ApiKeys::CONTROLLED_SHUTDOWN, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(4242, $request->getBrokerId());
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'brokerId'],
            array_keys(ControlledShutdownRequestV1::getScheme()),
            'the frame of version 1 ends with the broker id'
        );
    }

    public function testVersion0RequestHeaderCarriesNoClientId(): void
    {
        // Size = 12: ApiKey 7, ApiVersion 0, CorrelationId 20, BrokerId 4242 - and no ClientId string in between
        $request = new ControlledShutdownRequestV0(4242, 20);

        self::assertSame(
            '0000000c' . '0007' . '0000' . '00000014' . '00001092',
            bin2hex((string) $request)
        );
        self::assertSame(12, $request->getMessageSize());
        self::assertSame(ApiKeys::CONTROLLED_SHUTDOWN, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(4242, $request->getBrokerId());
    }

    public function testVersion0SchemeDropsTheClientIdOfTheCommonHeader(): void
    {
        // Reading a client id string as the broker id is exactly the bug this guards against
        self::assertArrayNotHasKey('clientId', ControlledShutdownRequestV0::getScheme());
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'brokerId'],
            array_keys(ControlledShutdownRequestV0::getScheme())
        );
    }

    public function testResponseWithoutRemainingPartitionsIsDecoded(): void
    {
        // The answer of a 0.9.0.1 broker for a broker id the controller does not know: error 8, no partitions
        $response = ControlledShutdownResponseV2::unpack(
            new StringStream((string) hex2bin('0000000a' . '00000014' . '0008' . '00000000'))
        );

        self::assertSame(20, $response->getCorrelationId());
        self::assertSame(KafkaException::BROKER_NOT_AVAILABLE, $response->errorCode);
        self::assertSame([], $response->remainingTopicPartitions);
    }

    public function testResponseKeepsEveryRemainingTopicPartitionOfTheFlatList(): void
    {
        //   ErrorCode 0, then ("orders", 0), ("orders", 2) - the same topic appears twice, the list is not grouped
        $frame = '00000022' . '00000001' . '0000' . '00000002'
            . '0006' . '6f7264657273' . '00000000'
            . '0006' . '6f7264657273' . '00000002';

        $response = ControlledShutdownResponseV2::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(0, $response->errorCode);
        self::assertCount(2, $response->remainingTopicPartitions);
        self::assertSame('orders', $response->remainingTopicPartitions[0]->topic);
        self::assertSame(0, $response->remainingTopicPartitions[0]->partition);
        self::assertSame('orders', $response->remainingTopicPartitions[1]->topic);
        self::assertSame(2, $response->remainingTopicPartitions[1]->partition);

        self::assertSame($frame, bin2hex((string) $response), 'the response has to survive a round trip');
    }
}
