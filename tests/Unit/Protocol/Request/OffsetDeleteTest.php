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
use Protocol\Kafka\Protocol\Data\OffsetDeleteRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetDeleteRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetDeleteResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetDeleteResponseTopic;
use Protocol\Kafka\Protocol\Request\OffsetDeleteRequest;
use Protocol\Kafka\Protocol\Request\OffsetDeleteResponse;

/**
 * Byte-exact tests for the OffsetDelete api (key 47, v0, Kafka 2.4, KIP-496).
 *
 * The api is the one thing Kafka 2.4 added **without** the flexible encoding of KIP-482, which makes these frames
 * the plain ones of every line below: int16-prefixed strings, int32-counted arrays, no tag buffer anywhere. The
 * one thing that is unusual about it is the order of the two fields the answer opens with - the top-level error
 * code stands **before** the throttle time, where every other api of this protocol has it the other way round.
 *
 * @see docs/protocol/2.8.md, section "OffsetDelete API (key 47, v0)"
 */
#[CoversClass(OffsetDeleteRequest::class)]
#[CoversClass(OffsetDeleteResponse::class)]
#[CoversClass(OffsetDeleteRequestTopic::class)]
#[CoversClass(OffsetDeleteRequestPartition::class)]
#[CoversClass(OffsetDeleteResponseTopic::class)]
#[CoversClass(OffsetDeleteResponsePartition::class)]
final class OffsetDeleteTest extends TestCase
{
    /**
     * OffsetDelete request v0 for the partitions 0 and 2 of one topic of the group `events-group`.
     *
     *   Size          => 00 00 00 34 (52 bytes)
     *   ApiKey        => 00 2f (47), ApiVersion => 00 00
     *   CorrelationId => 00 00 00 07
     *   ClientId      => 00 04 "test"
     *   GroupId       => 00 0c "events-group"
     *   Topics        => 00 00 00 01
     *     Name        => 00 06 "events"
     *     Partitions  => 00 00 00 02
     *       PartitionIndex => 00 00 00 00
     *       PartitionIndex => 00 00 00 02
     */
    private const string REQUEST_HEX = '00000034'
        . '002f'
        . '0000'
        . '00000007'
        . '0004' . '74657374'
        . '000c' . '6576656e74732d67726f7570'
        . '00000001'
        . '0006' . '6576656e7473'
        . '00000002'
        . '00000000'
        . '00000002';

    /**
     * The answer to it: the first partition deleted, the second one refused with the code 86.
     *
     *   Size           => 00 00 00 26 (38 bytes)
     *   CorrelationId  => 00 00 00 07
     *   ErrorCode      => 00 00                  (the top level, and it comes FIRST)
     *   ThrottleTimeMs => 00 00 00 00
     *   Topics         => 00 00 00 01, 00 06 "events", 00 00 00 02
     *     00 00 00 00, ErrorCode 00 00
     *     00 00 00 02, ErrorCode 00 56 (86)
     */
    private const string RESPONSE_HEX = '00000026'
        . '00000007'
        . '0000'
        . '00000000'
        . '00000001'
        . '0006' . '6576656e7473'
        . '00000002'
        . '00000000' . '0000'
        . '00000002' . '0056';

    /**
     * A group error answers no topic at all, and the whole frame is fourteen bytes.
     */
    private const string GROUP_ERROR_HEX = '0000000e'
        . '00000007'
        . '0045'
        . '00000000'
        . '00000000';

    public function testTheRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetDeleteRequest('events-group', ['events' => [0, 2]], 'test', 7);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_DELETE, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertSame('events-group', $request->getGroupId());
        self::assertFalse(
            OffsetDeleteRequest::isFlexible(),
            'the one api Kafka 2.4 added without the flexible encoding of KIP-482'
        );
        self::assertSame(
            OffsetDeleteRequest::HEADER_V1,
            $request->getHeaderVersion(),
            'so it keeps the plain request header, tag buffer and all'
        );
    }

    /**
     * A topic may be given as a prepared structure as well as a plain list of partition indexes
     */
    public function testATopicCanBeGivenAsAStructure(): void
    {
        $request = new OffsetDeleteRequest(
            'events-group',
            ['events' => new OffsetDeleteRequestTopic('events', [0, 2])],
            'test',
            7
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
    }

    /**
     * The empty topic array is a legal request that asks the coordinator about nothing
     */
    public function testTheEmptyTopicArrayIsALegalRequest(): void
    {
        $request = new OffsetDeleteRequest('events-group', [], 'test', 7);

        self::assertStringEndsWith('000c' . '6576656e74732d67726f7570' . '00000000', bin2hex((string) $request));
        self::assertSame([], $request->getTopics());
    }

    public function testTheAnswerCarriesOneErrorPerPartition(): void
    {
        $response = OffsetDeleteResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'the top level says nothing about them');
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['events'], array_keys($response->topics));

        $partitions = $response->topics['events']->partitions;

        self::assertSame([0, 2], array_keys($partitions), 'the answer is indexed by the partition index');
        self::assertSame(KafkaException::NO_ERROR, $partitions[0]->errorCode);
        self::assertSame(KafkaException::GROUP_SUBSCRIBED_TO_TOPIC, $partitions[2]->errorCode);
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The two fields the answer opens with are in the order no other api of this protocol has
     */
    public function testTheTopLevelErrorCodeStandsBeforeTheThrottleTime(): void
    {
        $response = OffsetDeleteResponse::unpack(new StringStream((string) hex2bin(self::GROUP_ERROR_HEX)));

        self::assertSame(KafkaException::GROUP_ID_NOT_FOUND, $response->errorCode);
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame([], $response->topics, 'a group error answers no topic at all');
        self::assertSame(self::GROUP_ERROR_HEX, bin2hex((string) $response));

        self::assertSame(
            ['errorCode', 'throttleTimeMs', 'topics'],
            array_slice(array_keys(OffsetDeleteResponse::getScheme()), -3),
            'the field order of OffsetDeleteResponse.json @ 2.8.2, which no other answer of this protocol has'
        );
    }
}
