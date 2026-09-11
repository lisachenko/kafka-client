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
use Protocol\Kafka\Protocol\Data\DescribeLogDirsRequestTopic;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDir;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseTopic;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequest;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponse;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponseV0;

/**
 * Byte-exact tests for the DescribeLogDirs API of Kafka 1.0 (api key 35, v0, KIP-113).
 *
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0 and v1)"
 */
#[CoversClass(DescribeLogDirsRequest::class)]
#[CoversClass(DescribeLogDirsRequestV0::class)]
#[CoversClass(DescribeLogDirsResponse::class)]
#[CoversClass(DescribeLogDirsResponseV0::class)]
#[CoversClass(DescribeLogDirsRequestTopic::class)]
#[CoversClass(DescribeLogDirsResponseLogDir::class)]
#[CoversClass(DescribeLogDirsResponseTopic::class)]
#[CoversClass(DescribeLogDirsResponsePartition::class)]
final class DescribeLogDirsTest extends TestCase
{
    /**
     * DescribeLogDirs request v1 for two partitions of one topic.
     *
     *   Size          => 00 00 00 25 (37 bytes)
     *   ApiKey        => 00 23 (35)
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 05
     *   ClientId      => 00 04 "test"
     *   Topics        => 00 00 00 01
     *     Topic      => 00 05 "topic"
     *     Partitions => 00 00 00 02, 00 00 00 00, 00 00 00 01
     */
    private const string REQUEST_HEX = '00000025'
        . '0023'
        . '0001'
        . '00000005'
        . '0004' . '74657374'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002' . '00000000' . '00000001';

    /**
     * The same request with a **null** topic array, which asks for every replica of every log directory.
     */
    private const string ALL_PARTITIONS_REQUEST_HEX = '00000012'
        . '0023'
        . '0001'
        . '00000005'
        . '0004' . '74657374'
        . 'ffffffff';

    /**
     * The same request with an **empty** topic array, which asks for no replica at all.
     */
    private const string NO_PARTITIONS_REQUEST_HEX = '00000012'
        . '0023'
        . '0001'
        . '00000005'
        . '0004' . '74657374'
        . '00000000';

    /**
     * DescribeLogDirs response v1 of a broker with two log directories: the second one holds the replica, and the
     * first one is offline, which is the only error code a directory of a 1.1.1 broker can carry besides -1.
     *
     *   Size           => 00 00 00 47 (71 bytes)
     *   CorrelationId  => 00 00 00 05
     *   ThrottleTimeMs => 00 00 00 00
     *   LogDirs        => 00 00 00 02
     *     ErrorCode => 00 38 (56, KafkaStorageError), LogDir => 00 05 "/disk", Topics => 00 00 00 00
     *     ErrorCode => 00 00,                        LogDir => 00 06 "/disk2", Topics => 00 00 00 01
     *       Topic      => 00 05 "topic"
     *       Partitions => 00 00 00 01
     *         PartitionId => 00 00 00 00, Size => 4096, OffsetLag => 0, IsFuture => 00
     */
    private const string RESPONSE_HEX = '00000047'
        . '00000005'
        . '00000000'
        . '00000002'
        . '0038' . '0005' . '2f6469736b' . '00000000'
        . '0000' . '0006' . '2f6469736b32' . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . '0000000000001000' . '0000000000000000' . '00';

    /**
     * The answer while a move runs: the same partition in both directories, the future one still 7 records behind.
     */
    private const string MOVING_RESPONSE_HEX = '00000067'
        . '00000005'
        . '00000000'
        . '00000002'
        . '0000' . '0005' . '2f6469736b' . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . '0000000000001000' . '0000000000000000' . '00'
        . '0000' . '0006' . '2f6469736b32' . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . '0000000000000800' . '0000000000000007' . '01';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DescribeLogDirsRequest(['topic' => [0, 1]], 'test', 5);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_LOG_DIRS, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised the api to version 1 (KIP-219)');
        self::assertSame(37, $request->getMessageSize());
    }

    public function testANullTopicArrayAsksForEveryReplicaOfEveryDirectory(): void
    {
        // `DescribeLogDirsRequest.isAllTopicPartitions` @ 1.1.1: the -1 size of the nullable array is what the
        // Java admin client and `kafka-log-dirs.sh --describe` always send
        $request = new DescribeLogDirsRequest(null, 'test', 5);

        self::assertSame(self::ALL_PARTITIONS_REQUEST_HEX, bin2hex((string) $request));
        self::assertStringEndsWith('ffffffff', self::ALL_PARTITIONS_REQUEST_HEX);
    }

    public function testTheDefaultRequestIsTheNullOne(): void
    {
        self::assertSame(
            self::ALL_PARTITIONS_REQUEST_HEX,
            bin2hex((string) new DescribeLogDirsRequest(clientId: 'test', correlationId: 5))
        );
    }

    public function testAnEmptyTopicArrayIsNotTheSameFrameAsANullOne(): void
    {
        // The empty array asks for NO replica, and is the cheapest way to ask which disks a broker has
        $request = new DescribeLogDirsRequest([], 'test', 5);

        self::assertSame(self::NO_PARTITIONS_REQUEST_HEX, bin2hex((string) $request));
        self::assertNotSame(self::ALL_PARTITIONS_REQUEST_HEX, self::NO_PARTITIONS_REQUEST_HEX);
    }

    public function testAlreadyBuiltTopicEntriesAreTakenAsTheyAre(): void
    {
        $built = new DescribeLogDirsRequest(['topic' => new DescribeLogDirsRequestTopic('topic', [0, 1])], 'test', 5);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $built));
    }

    public function testTheRequestSurvivesADecodeAndEncodeRoundTrip(): void
    {
        foreach ([self::REQUEST_HEX, self::ALL_PARTITIONS_REQUEST_HEX, self::NO_PARTITIONS_REQUEST_HEX] as $hex) {
            $decoded = DescribeLogDirsRequest::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($hex, bin2hex((string) $decoded));
        }
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DescribeLogDirsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(5, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(
            ['/disk', '/disk2'],
            array_keys($response->logDirs),
            'the directories are keyed by their absolute path'
        );

        $offline = $response->logDirs['/disk'];
        self::assertSame(KafkaException::KAFKA_STORAGE_ERROR, $offline->errorCode);
        self::assertSame([], $offline->topics, 'an offline directory reports no replica at all');

        $online = $response->logDirs['/disk2'];
        self::assertSame(KafkaException::NO_ERROR, $online->errorCode);
        self::assertSame(['topic'], array_keys($online->topics), 'topics are keyed by their name');

        $partition = $online->topics['topic']->partitions[0];
        self::assertSame(0, $partition->partition);
        self::assertSame(4096, $partition->size);
        self::assertSame(0, $partition->offsetLag, 'the current log of a healthy leader is never behind');
        self::assertFalse($partition->isFuture);
    }

    public function testAPartitionThatIsBeingMovedIsReportedInBothDirectories(): void
    {
        $response = DescribeLogDirsResponse::unpack(new StringStream((string) hex2bin(self::MOVING_RESPONSE_HEX)));

        $source      = $response->logDirs['/disk']->topics['topic']->partitions[0];
        $destination = $response->logDirs['/disk2']->topics['topic']->partitions[0];

        self::assertFalse($source->isFuture, 'the log the partition is still served from');
        self::assertSame(0, $source->offsetLag);
        self::assertTrue($destination->isFuture, 'the log `ReplicaAlterLogDirsThread` is filling');
        self::assertSame(7, $destination->offsetLag, 'the records the mover still has to copy');
        self::assertLessThan($source->size, $destination->size, 'the future log is still smaller');
    }

    public function testTheResponseSurvivesADecodeAndEncodeRoundTrip(): void
    {
        foreach ([self::RESPONSE_HEX, self::MOVING_RESPONSE_HEX] as $hex) {
            $decoded = DescribeLogDirsResponse::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($hex, bin2hex((string) $decoded));
        }
    }

    public function testTheInvalidOffsetLagIsMinusOne(): void
    {
        // `DescribeLogDirsResponse.INVALID_OFFSET_LAG` @ 1.1.1, the lag of a replica the broker does not have
        self::assertSame(-1, DescribeLogDirsResponsePartition::INVALID_OFFSET_LAG);
    }

    public function testTheVersionZeroFrameIsTheSameBodyWithALowerVersionField(): void
    {
        $request = new DescribeLogDirsRequestV0(['topic' => [0, 1]], 'test', 5);

        // `DESCRIBE_LOG_DIRS_REQUEST_V1 = DESCRIBE_LOG_DIRS_REQUEST_V0` @ 2.0.1
        self::assertSame(substr_replace(self::REQUEST_HEX, '0000', 12, 4), bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());

        $response = DescribeLogDirsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

}
