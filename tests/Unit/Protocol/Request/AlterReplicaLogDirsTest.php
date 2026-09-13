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
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsRequestLogDir;
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsRequestTopic;
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsResponsePartition;
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsResponseTopic;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsRequest;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsRequestV0;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsRequestV1;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsResponse;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsResponseV0;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsResponseV1;

/**
 * Byte-exact tests for the AlterReplicaLogDirs API of Kafka 1.0 (api key 34, v0, KIP-113).
 *
 * @see docs/protocol/2.8.md, section "AlterReplicaLogDirs API (key 34, v0 to v2)"
 */
#[CoversClass(AlterReplicaLogDirsRequest::class)]
#[CoversClass(AlterReplicaLogDirsRequestV1::class)]
#[CoversClass(AlterReplicaLogDirsRequestV0::class)]
#[CoversClass(AlterReplicaLogDirsResponse::class)]
#[CoversClass(AlterReplicaLogDirsResponseV1::class)]
#[CoversClass(AlterReplicaLogDirsResponseV0::class)]
#[CoversClass(AlterReplicaLogDirsRequestLogDir::class)]
#[CoversClass(AlterReplicaLogDirsRequestTopic::class)]
#[CoversClass(AlterReplicaLogDirsResponseTopic::class)]
#[CoversClass(AlterReplicaLogDirsResponsePartition::class)]
final class AlterReplicaLogDirsTest extends TestCase
{
    /**
     * AlterReplicaLogDirs request v1 that moves two partitions of one topic into one directory.
     *
     *   Size          => 00 00 00 31 (49 bytes)
     *   ApiKey        => 00 22 (34)
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 05
     *   ClientId      => 00 04 "test"
     *   LogDirs       => 00 00 00 01
     *     LogDir => 00 06 "/disk2"
     *     Topics => 00 00 00 01
     *       Topic      => 00 05 "topic"
     *       Partitions => 00 00 00 02, 00 00 00 00, 00 00 00 01
     */
    private const string REQUEST_HEX = '00000031'
        . '0022'
        . '0001'
        . '00000005'
        . '0004' . '74657374'
        . '00000001'
        . '0006' . '2f6469736b32'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002' . '00000000' . '00000001';

    /**
     * AlterReplicaLogDirs response v1 of two replicas: one accepted, one refused with 57 (LogDirNotFound).
     *
     *   Size           => 00 00 00 23 (35 bytes)
     *   CorrelationId  => 00 00 00 05
     *   ThrottleTimeMs => 00 00 00 00
     *   Topics         => 00 00 00 01
     *     Topic      => 00 05 "topic"
     *     Partitions => 00 00 00 02
     *       PartitionId => 00 00 00 00, ErrorCode => 00 00
     *       PartitionId => 00 00 00 01, ErrorCode => 00 39 (57)
     */
    private const string RESPONSE_HEX = '00000023'
        . '00000005'
        . '00000000'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000'
        . '00000001' . '0039';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new AlterReplicaLogDirsRequestV1(['/disk2' => ['topic' => [0, 1]]], 'test', 5);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::ALTER_REPLICA_LOG_DIRS, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised the api to version 1 (KIP-219)');
        self::assertSame(49, $request->getMessageSize());
    }

    public function testAlreadyBuiltEntriesAreTakenAsTheyAre(): void
    {
        $built = new AlterReplicaLogDirsRequestV1(
            [
                '/disk2' => new AlterReplicaLogDirsRequestLogDir('/disk2', [
                    'topic' => new AlterReplicaLogDirsRequestTopic('topic', [0, 1]),
                ]),
            ],
            'test',
            5
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $built));
    }

    public function testTheRequestIsGroupedByDestinationDirectory(): void
    {
        // `AlterReplicaLogDirsRequest.toStruct` @ 1.1.1 inverts the `Map<TopicPartition, String>` of the caller,
        // so two replicas that go to two directories are two entries of the top-level array
        $request = new AlterReplicaLogDirsRequestV1(
            ['/disk1' => ['topic' => [0]], '/disk2' => ['topic' => [1]]],
            'test',
            5
        );

        $frame = bin2hex((string) $request);
        self::assertStringContainsString('00000002' . '0006' . '2f6469736b31', $frame, 'two log directories');
        self::assertStringContainsString('0006' . '2f6469736b32', $frame);
    }

    public function testTheRequestSurvivesADecodeAndEncodeRoundTrip(): void
    {
        $decoded = AlterReplicaLogDirsRequestV1::unpack(new StringStream((string) hex2bin(self::REQUEST_HEX)));

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $decoded));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = AlterReplicaLogDirsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(5, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['topic'], array_keys($response->topics), 'topics are keyed by their name');

        $partitions = $response->topics['topic']->partitions;
        self::assertSame([0, 1], array_keys($partitions), 'partitions are keyed by their id');
        self::assertSame(
            KafkaException::NO_ERROR,
            $partitions[0]->errorCode,
            'a 0 only says that the move was accepted, not that it is finished'
        );
        self::assertSame(KafkaException::LOG_DIR_NOT_FOUND, $partitions[1]->errorCode);
    }

    public function testTheResponseSurvivesADecodeAndEncodeRoundTrip(): void
    {
        $decoded = AlterReplicaLogDirsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $decoded));
    }

    public function testTheVersionZeroFrameIsTheSameBodyWithALowerVersionField(): void
    {
        $request = new AlterReplicaLogDirsRequestV0(['/disk2' => ['topic' => [0, 1]]], 'test', 5);

        // `ALTER_REPLICA_LOG_DIRS_REQUEST_V1 = ALTER_REPLICA_LOG_DIRS_REQUEST_V0` @ 2.0.1
        self::assertSame(substr_replace(self::REQUEST_HEX, '0000', 12, 4), bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());

        $response = AlterReplicaLogDirsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

}
