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
use Protocol\Kafka\Protocol\Data\DeleteRecordsRequestPartition;
use Protocol\Kafka\Protocol\Data\DeleteRecordsRequestTopic;
use Protocol\Kafka\Protocol\Data\DeleteRecordsResponsePartition;
use Protocol\Kafka\Protocol\Data\DeleteRecordsResponseTopic;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequestV0;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequestV1;
use Protocol\Kafka\Protocol\Request\DeleteRecordsResponse;
use Protocol\Kafka\Protocol\Request\DeleteRecordsResponseV0;
use Protocol\Kafka\Protocol\Request\DeleteRecordsResponseV1;

/**
 * Byte-exact tests for the DeleteRecords API of Kafka 0.11 (api key 21, v0).
 *
 * @see docs/protocol/2.8.md, section "DeleteRecords API (key 21, v0 to v2)"
 */
#[CoversClass(DeleteRecordsRequest::class)]
#[CoversClass(DeleteRecordsRequestV0::class)]
#[CoversClass(DeleteRecordsResponse::class)]
#[CoversClass(DeleteRecordsResponseV0::class)]
#[CoversClass(DeleteRecordsRequestTopic::class)]
#[CoversClass(DeleteRecordsRequestPartition::class)]
#[CoversClass(DeleteRecordsResponseTopic::class)]
#[CoversClass(DeleteRecordsResponsePartition::class)]
final class DeleteRecordsTest extends TestCase
{
    /**
     * DeleteRecords request v0 for two partitions of one topic.
     *
     *   Size          => 00 00 00 39 (57 bytes)
     *   ApiKey        => 00 15 (21)
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 05
     *   ClientId      => 00 04 "test"
     *   Topics        => 00 00 00 01
     *     Topic      => 00 05 "topic"
     *     Partitions => 00 00 00 02
     *       PartitionId => 00 00 00 00, Offset => 00 00 00 00 00 00 00 02
     *       PartitionId => 00 00 00 01, Offset => ff ff ff ff ff ff ff ff (HIGH_WATERMARK)
     *   Timeout       => 00 00 75 30 (30000)
     */
    private const string REQUEST_HEX = '00000039'
        . '0015'
        . '0001'
        . '00000005'
        . '0004' . '74657374'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000002'
        . '00000001' . 'ffffffffffffffff'
        . '00007530';

    /**
     * DeleteRecords response v1: one partition deleted, one that the broker does not lead.
     *
     *   Size           => 00 00 00 33 (51 bytes)
     *   CorrelationId  => 00 00 00 05
     *   ThrottleTimeMs => 00 00 00 00
     *   Topics         => 00 00 00 01
     *     Topic      => 00 05 "topic"
     *     Partitions => 00 00 00 02
     *       PartitionId => 00 00 00 00, LowWatermark => 2,  ErrorCode => 00 00
     *       PartitionId => 00 00 00 01, LowWatermark => -1, ErrorCode => 00 06
     */
    private const string RESPONSE_HEX = '00000033'
        . '00000005'
        . '00000000'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000002' . '0000'
        . '00000001' . 'ffffffffffffffff' . '0006';

    /**
     * The same answer of a broker that delayed it because of a quota: `ThrottleTimeMs` of 793 ms
     */
    private const string THROTTLED_RESPONSE_HEX = '00000033'
        . '00000005'
        . '00000319'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000002' . '0000'
        . '00000001' . 'ffffffffffffffff' . '0006';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DeleteRecordsRequestV1(
            ['topic' => [0 => 2, 1 => DeleteRecordsRequest::HIGH_WATERMARK]],
            30000,
            'test',
            5
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DELETE_RECORDS, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised the api to version 1 (KIP-219)');
        self::assertSame(57, $request->getMessageSize());
    }

    public function testTheHighWatermarkIsMinusOne(): void
    {
        // `DeleteRecordsRequest.HIGH_WATERMARK` @ 0.11.0.3, the only special offset value of the api
        self::assertSame(-1, DeleteRecordsRequest::HIGH_WATERMARK);
    }

    public function testAlreadyBuiltTopicEntriesAreTakenAsTheyAre(): void
    {
        $built = new DeleteRecordsRequestV1(
            ['topic' => new DeleteRecordsRequestTopic('topic', [
                0 => new DeleteRecordsRequestPartition(0, 2),
                1 => new DeleteRecordsRequestPartition(1),
            ])],
            30000,
            'test',
            5
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $built));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DeleteRecordsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(5, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['topic'], array_keys($response->topics), 'topics are keyed by their name');

        $partitions = $response->topics['topic']->partitions;
        self::assertSame([0, 1], array_keys($partitions), 'partitions are keyed by their id');
        self::assertSame(KafkaException::NO_ERROR, $partitions[0]->errorCode);
        self::assertSame(2, $partitions[0]->lowWatermark, 'the offset of the first record that is still readable');
        self::assertSame(KafkaException::NOT_LEADER_FOR_PARTITION, $partitions[1]->errorCode);
        self::assertSame(
            DeleteRecordsResponsePartition::INVALID_LOW_WATERMARK,
            $partitions[1]->lowWatermark,
            'a partition that failed carries -1 instead of a watermark'
        );
    }

    public function testTheThrottleTimeOpensTheAnswerOfThisApi(): void
    {
        // The api was born after KIP-124, so there is no version of it without a leading throttle time
        $response = DeleteRecordsResponseV1::unpack(new StringStream((string) hex2bin(self::THROTTLED_RESPONSE_HEX)));

        self::assertSame(793, $response->throttleTimeMs);
        self::assertSame(2, $response->topics['topic']->partitions[0]->lowWatermark);
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $response = DeleteRecordsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testVersionTwoIsTheSameFrameInTheFlexibleEncoding(): void
    {
        // KIP-482 (Kafka 2.6): `DeleteRecordsRequest.json` @ 2.8.2 says "Version 2 is the first flexible
        // version" and declares no field of it. The request header v2 gains a tag buffer behind the client id,
        // the strings and arrays become compact, and the body, every topic entry and every partition entry end
        // in a tagged-field section
        $request = new DeleteRecordsRequest(
            ['topic' => [0 => 2, 1 => DeleteRecordsRequest::HIGH_WATERMARK]],
            30000,
            'test',
            5
        );

        self::assertSame(
            '00000037'
            . '0015' . '0002' . '00000005'
            . '0004' . '74657374'                          // the client id is never compact
            . '00'                                         // the tag buffer of the request header v2
            . '02'                                         // the topics array: the varint 1 + 1
            . '06' . '746f706963'                          // the name: the varint 5 + 1
            . '03'                                         // the partitions array: the varint 2 + 1
            . '00000000' . '0000000000000002' . '00'       // partition 0, offset 2, tag buffer
            . '00000001' . 'ffffffffffffffff' . '00'       // partition 1, HIGH_WATERMARK, tag buffer
            . '00'                                         // the tag buffer of the topic entry
            . '00007530'                                   // the timeout, behind the topics as in every version
            . '00',                                        // the tag buffer of the body
            bin2hex((string) $request)
        );
        self::assertTrue(DeleteRecordsRequest::isFlexible());
        self::assertSame(2, DeleteRecordsRequest::FLEXIBLE_VERSION);
        self::assertSame(2, DeleteRecordsRequest::VERSION);
        self::assertSame(2, DeleteRecordsResponse::VERSION);
        self::assertFalse(DeleteRecordsRequestV1::isFlexible(), 'version 1 is the last plain one');
        self::assertSame(1, DeleteRecordsRequestV1::VERSION);
        self::assertSame(1, DeleteRecordsResponseV1::VERSION);

        // The frame of version 1 is the same question with int16 and int32 lengths and no section at all
        self::assertSame(self::REQUEST_HEX, bin2hex((string) new DeleteRecordsRequestV1(
            ['topic' => [0 => 2, 1 => DeleteRecordsRequest::HIGH_WATERMARK]],
            30000,
            'test',
            5
        )));
    }

    public function testTheVersionZeroFrameIsTheSameBodyWithALowerVersionField(): void
    {
        $request = new DeleteRecordsRequestV0(
            ['topic' => [0 => 2, 1 => DeleteRecordsRequest::HIGH_WATERMARK]],
            30000,
            'test',
            5
        );

        // `DELETE_RECORDS_REQUEST_V1 = DELETE_RECORDS_REQUEST_V0` @ 2.0.1
        self::assertSame(substr_replace(self::REQUEST_HEX, '0000', 12, 4), bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());

        $response = DeleteRecordsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

}
