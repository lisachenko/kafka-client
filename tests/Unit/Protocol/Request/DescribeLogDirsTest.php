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
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDirV3;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseTopic;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequest;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequestV2;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequestV3;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponse;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponseV1;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponseV2;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponseV3;

/**
 * Byte-exact tests for the DescribeLogDirs API of Kafka 1.0 (api key 35, v0, KIP-113), for the top-level error
 * code its version 3 gained in Kafka 3.2 and for the volume sizes of KIP-827 that its version 4 gained in Kafka
 * 3.3.
 *
 * @see docs/protocol/3.9.md, section "DescribeLogDirs API (key 35, v0 to v4)"
 */
#[CoversClass(DescribeLogDirsRequest::class)]
#[CoversClass(DescribeLogDirsRequestV3::class)]
#[CoversClass(DescribeLogDirsRequestV2::class)]
#[CoversClass(DescribeLogDirsRequestV1::class)]
#[CoversClass(DescribeLogDirsRequestV0::class)]
#[CoversClass(DescribeLogDirsResponse::class)]
#[CoversClass(DescribeLogDirsResponseV3::class)]
#[CoversClass(DescribeLogDirsResponseV2::class)]
#[CoversClass(DescribeLogDirsResponseV1::class)]
#[CoversClass(DescribeLogDirsResponseV0::class)]
#[CoversClass(DescribeLogDirsRequestTopic::class)]
#[CoversClass(DescribeLogDirsResponseLogDir::class)]
#[CoversClass(DescribeLogDirsResponseLogDirV3::class)]
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
        $request = new DescribeLogDirsRequestV1(['topic' => [0, 1]], 'test', 5);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_LOG_DIRS, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised the api to version 1 (KIP-219)');
        self::assertSame(37, $request->getMessageSize());
    }

    public function testANullTopicArrayAsksForEveryReplicaOfEveryDirectory(): void
    {
        // `DescribeLogDirsRequest.isAllTopicPartitions` @ 1.1.1: the -1 size of the nullable array is what the
        // Java admin client and `kafka-log-dirs.sh --describe` always send
        $request = new DescribeLogDirsRequestV1(null, 'test', 5);

        self::assertSame(self::ALL_PARTITIONS_REQUEST_HEX, bin2hex((string) $request));
        self::assertStringEndsWith('ffffffff', self::ALL_PARTITIONS_REQUEST_HEX);
    }

    public function testTheDefaultRequestIsTheNullOne(): void
    {
        self::assertSame(
            self::ALL_PARTITIONS_REQUEST_HEX,
            bin2hex((string) new DescribeLogDirsRequestV1(clientId: 'test', correlationId: 5))
        );
    }

    public function testAnEmptyTopicArrayIsNotTheSameFrameAsANullOne(): void
    {
        // The empty array asks for NO replica, and is the cheapest way to ask which disks a broker has
        $request = new DescribeLogDirsRequestV1([], 'test', 5);

        self::assertSame(self::NO_PARTITIONS_REQUEST_HEX, bin2hex((string) $request));
        self::assertNotSame(self::ALL_PARTITIONS_REQUEST_HEX, self::NO_PARTITIONS_REQUEST_HEX);
    }

    public function testAlreadyBuiltTopicEntriesAreTakenAsTheyAre(): void
    {
        $built = new DescribeLogDirsRequestV1(['topic' => new DescribeLogDirsRequestTopic('topic', [0, 1])], 'test', 5);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $built));
    }

    public function testTheRequestSurvivesADecodeAndEncodeRoundTrip(): void
    {
        foreach ([self::REQUEST_HEX, self::ALL_PARTITIONS_REQUEST_HEX, self::NO_PARTITIONS_REQUEST_HEX] as $hex) {
            $decoded = DescribeLogDirsRequestV1::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($hex, bin2hex((string) $decoded));
        }
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DescribeLogDirsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

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
        $response = DescribeLogDirsResponseV1::unpack(new StringStream((string) hex2bin(self::MOVING_RESPONSE_HEX)));

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
            $decoded = DescribeLogDirsResponseV1::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($hex, bin2hex((string) $decoded));
        }
    }

    public function testTheInvalidOffsetLagIsMinusOne(): void
    {
        // `DescribeLogDirsResponse.INVALID_OFFSET_LAG` @ 1.1.1, the lag of a replica the broker does not have
        self::assertSame(-1, DescribeLogDirsResponsePartition::INVALID_OFFSET_LAG);
    }

    /**
     * The refusal of the whole request the version 3 of Kafka 3.2 added, as the node answered it to `acltest`.
     *
     *   Size           => 00 00 00 0d (13 bytes)
     *   CorrelationId  => 00 00 0c e7, then the tag buffer of the response header v1
     *   ThrottleTimeMs => 00 00 00 00
     *   ErrorCode      => 00 1f (31, ClusterAuthorizationFailed)   -- the field of the version 3
     *   LogDirs        => 01 (the empty compact array), then the tag buffer of the body
     */
    private const string REFUSAL_V3_HEX = '0000000d' . '00000ce7' . '00' . '00000000' . '001f' . '01' . '00';

    /**
     * The same refusal of the version 2, which has no field for it: the empty directory array alone.
     */
    private const string REFUSAL_V2_HEX = '0000000b' . '00000ce8' . '00' . '00000000' . '01' . '00';

    /**
     * A version 4 answer with one empty directory, the frame that carries the two sizes of KIP-827.
     *
     *   Size / CorrelationId / the tag buffer of the response header v1
     *   ThrottleTimeMs => 00 00 00 00, ErrorCode => 00 00
     *   LogDirs        => 02 (one entry): the code 0, the compact path, an empty topic array,
     *                     TotalBytes  => 00 00 00 3e fe 39 d0 00 (270553174016)
     *                     UsableBytes => 00 00 00 05 5e 88 e0 00 (23060865024)
     */
    private const string SIZED_RESPONSE_V4_HEX = '00000031' . '00000ce9' . '00' . '00000000' . '0000' . '02'
        . '0000' . '102f746d702f6b61666b612d6c6f6773' . '01'
        . '0000003efe39d000' . '000000055e88e000' . '00' . '00';

    public function testTheVersionsTwoThreeAndFourOfTheRequestAreTheSameFrame(): void
    {
        // "Version 3 is the same as version 2 (new field in response)" of `DescribeLogDirsRequest.json` @ 3.2.3,
        // and "Version 4 is the same as version 2 (new fields in response)" of the same file @ 3.3.2
        $version4 = bin2hex((string) new DescribeLogDirsRequest(['topic' => [0, 1]], 'test', 5));
        $version3 = bin2hex((string) new DescribeLogDirsRequestV3(['topic' => [0, 1]], 'test', 5));
        $version2 = bin2hex((string) new DescribeLogDirsRequestV2(['topic' => [0, 1]], 'test', 5));

        self::assertSame($version3, substr_replace($version4, '0003', 12, 4));
        self::assertSame($version2, substr_replace($version4, '0002', 12, 4));
        self::assertSame(4, new DescribeLogDirsRequest(clientId: 'test')->getApiVersion());
        self::assertSame(3, new DescribeLogDirsRequestV3(clientId: 'test')->getApiVersion());
        self::assertSame(2, new DescribeLogDirsRequestV2(clientId: 'test')->getApiVersion());
    }

    public function testTheVersionFourAnswerCarriesTheVolumeSizesOfEveryDirectory(): void
    {
        $answer = DescribeLogDirsResponse::unpack(new StringStream((string) hex2bin(self::SIZED_RESPONSE_V4_HEX)));

        $directory = $answer->logDirs['/tmp/kafka-logs'];
        self::assertSame(270553174016, $directory->totalBytes, 'File.getTotalSpace of the volume');
        self::assertSame(23060865024, $directory->usableBytes, 'File.getUsableSpace of the volume');
        self::assertSame([], $directory->topics, 'the two sizes stand BEHIND the topics of the entry');
        self::assertSame(self::SIZED_RESPONSE_V4_HEX, bin2hex((string) $answer), 'and survive a round trip');
    }

    public function testTheVersionThreeAnswerHasNoFieldForThoseSizes(): void
    {
        $answer = DescribeLogDirsResponseV3::unpack(new StringStream((string) hex2bin(self::REFUSAL_V3_HEX)));

        self::assertSame([], $answer->logDirs);
        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $answer->errorCode);
        self::assertSame(
            DescribeLogDirsResponseLogDir::UNKNOWN_BYTES,
            DescribeLogDirsResponseLogDirV3::UNKNOWN_BYTES,
            'a directory entry below the version 4 keeps the -1 of the two fields of KIP-827'
        );
        self::assertArrayNotHasKey(
            'totalBytes',
            DescribeLogDirsResponseLogDirV3::getScheme(),
            'because the scheme of the version 3 entry does not declare them at all'
        );
    }

    public function testTheVersionThreeAnswerCarriesTheErrorCodeOfTheWholeRequest(): void
    {
        $refused = DescribeLogDirsResponse::unpack(new StringStream((string) hex2bin(self::REFUSAL_V3_HEX)));

        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $refused->errorCode);
        self::assertSame([], $refused->logDirs, 'a refusal carries no directory at all');
        self::assertSame(0, $refused->throttleTimeMs);
        self::assertSame(self::REFUSAL_V3_HEX, bin2hex((string) $refused), 'and the frame survives a round trip');
    }

    public function testTheVersionTwoAnswerHasNoFieldForThatErrorCode(): void
    {
        $refused = DescribeLogDirsResponseV2::unpack(new StringStream((string) hex2bin(self::REFUSAL_V2_HEX)));

        self::assertSame([], $refused->logDirs);
        self::assertSame(
            KafkaException::NO_ERROR,
            $refused->errorCode,
            'the property stays at its default: below the version 3 the refusal is the empty array alone'
        );
        self::assertSame(self::REFUSAL_V2_HEX, bin2hex((string) $refused));
        self::assertSame(
            strlen(self::REFUSAL_V3_HEX) - 4,
            strlen(self::REFUSAL_V2_HEX),
            'which is exactly the two bytes of the int16 less'
        );
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
