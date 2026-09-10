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
use Protocol\Kafka\Protocol\Request\DeleteRecordsResponse;

/**
 * Byte-exact tests for the DeleteRecords API of Kafka 0.11 (api key 21, v0).
 *
 * @see docs/protocol/1.1.md, section "DeleteRecords API (key 21, v0)"
 */
#[CoversClass(DeleteRecordsRequest::class)]
#[CoversClass(DeleteRecordsResponse::class)]
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
     *   ApiVersion    => 00 00
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
        . '0000'
        . '00000005'
        . '0004' . '74657374'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000002'
        . '00000001' . 'ffffffffffffffff'
        . '00007530';

    /**
     * DeleteRecords response v0: one partition deleted, one that the broker does not lead.
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
        $request = new DeleteRecordsRequest(
            ['topic' => [0 => 2, 1 => DeleteRecordsRequest::HIGH_WATERMARK]],
            30000,
            'test',
            5
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DELETE_RECORDS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'a 0.11.0.3 broker only serves version 0');
        self::assertSame(57, $request->getMessageSize());
    }

    public function testTheHighWatermarkIsMinusOne(): void
    {
        // `DeleteRecordsRequest.HIGH_WATERMARK` @ 0.11.0.3, the only special offset value of the api
        self::assertSame(-1, DeleteRecordsRequest::HIGH_WATERMARK);
    }

    public function testAlreadyBuiltTopicEntriesAreTakenAsTheyAre(): void
    {
        $built = new DeleteRecordsRequest(
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
        $response = DeleteRecordsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

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
        $response = DeleteRecordsResponse::unpack(new StringStream((string) hex2bin(self::THROTTLED_RESPONSE_HEX)));

        self::assertSame(793, $response->throttleTimeMs);
        self::assertSame(2, $response->topics['topic']->partitions[0]->lowWatermark);
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $response = DeleteRecordsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }
}
