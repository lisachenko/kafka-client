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
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetsRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopic;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;

/**
 * Byte-exact tests for the Offsets (ListOffset) API v0.
 *
 * <pre>
 *   OffsetRequest  => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
 *   OffsetResponse => [TopicName [Partition ErrorCode [Offset]]]
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "Offsets API (key 2, v0), a.k.a. ListOffset"
 */
#[CoversClass(OffsetsRequest::class)]
#[CoversClass(OffsetsResponse::class)]
#[CoversClass(OffsetsRequestTopic::class)]
#[CoversClass(OffsetsRequestPartition::class)]
#[CoversClass(OffsetsResponseTopic::class)]
#[CoversClass(OffsetsResponsePartition::class)]
final class OffsetsApiTest extends TestCase
{
    /**
     * Offsets request v0 asking for the latest offset of "topic-0", client id "test", correlation id 7.
     *
     *   Size               => 00 00 00 31 (49 bytes)
     *   ApiKey             => 00 02
     *   ApiVersion         => 00 00
     *   CorrelationId      => 00 00 00 07
     *   ClientId           => 00 04 "test"
     *   ReplicaId          => ff ff ff ff (-1, an ordinary consumer)
     *   [TopicName]        => 00 00 00 01, 00 05 "topic"
     *     [Partition]      => 00 00 00 01
     *       Partition          => 00 00 00 00
     *       Time               => ff ff ff ff ff ff ff ff (-1, the latest offset)
     *       MaxNumberOfOffsets => 00 00 00 01
     */
    private const string LATEST_REQUEST_HEX = '00000031'
        . '0002'
        . '0000'
        . '00000007'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . 'ffffffffffffffff' . '00000001';

    /**
     * The same request asking for the earliest available offset: Time is -2 instead of -1
     */
    private const string EARLIEST_REQUEST_HEX = '00000031'
        . '0002'
        . '0000'
        . '00000007'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . 'fffffffffffffffe' . '00000001';

    public function testLatestOffsetRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetsRequest(['topic' => [0 => OffsetsRequest::LATEST]], 1, -1, 'test', 7);

        self::assertSame(self::LATEST_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(49, $request->getMessageSize());
    }

    public function testEarliestOffsetRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetsRequest(['topic' => [0 => OffsetsRequest::EARLIEST]], 1, -1, 'test', 7);

        self::assertSame(self::EARLIEST_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testSpecialTimeValuesFollowTheSpec(): void
    {
        self::assertSame(-1, OffsetsRequest::LATEST);
        self::assertSame(-2, OffsetsRequest::EARLIEST);
    }

    public function testRequestAcceptsStructuredTopicPartitions(): void
    {
        $request = OffsetsRequest::fromTopicPartitions(
            [new TopicPartition('topic', 0)],
            OffsetsRequest::EARLIEST,
            1,
            -1,
            'test',
            7
        );

        self::assertSame(self::EARLIEST_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testRequestPacksEveryPartitionWithItsOwnTime(): void
    {
        $request = new OffsetsRequest(
            ['topic' => [0 => OffsetsRequest::EARLIEST, 3 => 1451606400000]],
            5,
            -1,
            'test',
            7
        );

        self::assertSame(
            '00000041'
            . '0002' . '0000' . '00000007' . '0004' . '74657374'
            . 'ffffffff'
            . '00000001'
            . '0005' . '746f706963'
            . '00000002'
            . '00000000' . 'fffffffffffffffe' . '00000005'
            . '00000003' . '00000151fa7bdc00' . '00000005',
            bin2hex((string) $request)
        );
    }

    public function testRequestSchemeOnlyDeclaresTheFieldsOfVersionZero(): void
    {
        $scheme = OffsetsRequest::getScheme();

        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'topicPartitions'],
            array_keys($scheme)
        );
        self::assertSame(['topic' => OffsetsRequestTopic::class], $scheme['topicPartitions']);
        self::assertSame(
            [
                'partition'          => BinarySchema::TYPE_INT32,
                'timestamp'          => BinarySchema::TYPE_INT64,
                'maxNumberOfOffsets' => BinarySchema::TYPE_INT32,
            ],
            OffsetsRequestPartition::getScheme()
        );
    }

    public function testResponseWithThreeSegmentOffsetsIsUnpacked(): void
    {
        // Size = 53: CorrelationId + one topic "topic" + one partition without an error and three int64 offsets
        $frame = (string) hex2bin(
            '00000035'
            . '00000007'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '0000'
            . '00000003'
            . '00000000000003e8' . '00000000000001f4' . '0000000000000000'
        );

        $response = OffsetsResponse::unpack(new StringStream($frame));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(['topic'], array_keys($response->topics));

        $partition = $response->topics['topic']->partitions[0];
        self::assertSame(0, $partition->partition);
        self::assertSame(0, $partition->errorCode);
        self::assertSame([1000, 500, 0], $partition->offsets, 'the newest segment offset comes first');
    }

    public function testResponseCarriesThePerPartitionErrorCodeAndNoOffsets(): void
    {
        // Error code 1 is OffsetOutOfRange; the offset array of a failed partition is empty
        $frame = (string) hex2bin(
            '0000001d'
            . '00000007'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000002'
            . '0001'
            . '00000000'
        );

        $partition = OffsetsResponse::unpack(new StringStream($frame))->topics['topic']->partitions[2];

        self::assertSame(2, $partition->partition);
        self::assertSame(1, $partition->errorCode);
        self::assertSame([], $partition->offsets);
    }
}
