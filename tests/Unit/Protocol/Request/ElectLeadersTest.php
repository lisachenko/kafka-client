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
use Protocol\Kafka\Admin\ElectionType;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ElectLeadersRequestTopicPartitions;
use Protocol\Kafka\Protocol\Data\ElectLeadersResponsePartitionResult;
use Protocol\Kafka\Protocol\Data\ElectLeadersResponseReplicaElectionResult;
use Protocol\Kafka\Protocol\Request\ElectLeadersRequest;
use Protocol\Kafka\Protocol\Request\ElectLeadersRequestV0;
use Protocol\Kafka\Protocol\Request\ElectLeadersResponse;
use Protocol\Kafka\Protocol\Request\ElectLeadersResponseV0;

/**
 * Byte-exact tests for the ElectLeaders API of Kafka 2.2 (api key 43, v0, KIP-183).
 *
 * The api was called **ElectPreferredLeaders** when it arrived and can ask for the preferred replica and nothing
 * else: the `election_type` byte of {@see ElectionType} is a field of the version 1 that Kafka 2.4 adds, and so is
 * the top-level error code of the answer. Everything this version reports is per partition.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 and v1)"
 */
#[CoversClass(ElectLeadersRequest::class)]
#[CoversClass(ElectLeadersRequestV0::class)]
#[CoversClass(ElectLeadersResponse::class)]
#[CoversClass(ElectLeadersResponseV0::class)]
#[CoversClass(ElectLeadersRequestTopicPartitions::class)]
#[CoversClass(ElectLeadersResponseReplicaElectionResult::class)]
#[CoversClass(ElectLeadersResponsePartitionResult::class)]
#[CoversClass(ElectionType::class)]
final class ElectLeadersTest extends TestCase
{
    /**
     * ElectLeaders request v0 for one named partition, with a timeout of 30 seconds.
     *
     *   Size            => 00 00 00 33 (51 bytes)
     *   ApiKey          => 00 2b (43)
     *   ApiVersion      => 00 00
     *   CorrelationId   => 00 00 02 be (702)
     *   ClientId        => 00 0a "t4-vectors"
     *   TopicPartitions => 00 00 00 01
     *     Topic       => 00 0d "t4-22-vectors"
     *     PartitionId => 00 00 00 01, 00 00 00 00
     *   TimeoutMs       => 00 00 75 30 (30000)
     */
    private const string REQUEST_HEX = '00000033'
        . '002b'
        . '0000'
        . '000002be'
        . '000a' . '74342d766563746f7273'
        . '00000001'
        . '000d' . '74342d32322d766563746f7273'
        . '00000001' . '00000000'
        . '00007530';

    /**
     * The same request with a **null** topic array, which asks the controller to look at every partition.
     */
    private const string ALL_PARTITIONS_REQUEST_HEX = '0000001c'
        . '002b'
        . '0000'
        . '000002be'
        . '000a' . '74342d766563746f7273'
        . 'ffffffff'
        . '00007530';

    /**
     * ElectLeaders answer v0 for a partition that is already led by its preferred replica: the error code 84.
     *
     *   Size           => 00 00 00 56 (86 bytes)
     *   CorrelationId  => 00 00 02 be
     *   ThrottleTimeMs => 00 00 00 00
     *   Results        => 00 00 00 01
     *     Topic           => 00 0d "t4-22-vectors"
     *     PartitionResult => 00 00 00 01
     *       PartitionId => 00 00 00 00, ErrorCode => 00 54 (84), ErrorMessage => 00 2f "Leader election not …"
     */
    private const string RESPONSE_HEX = '00000056'
        . '000002be'
        . '00000000'
        . '00000001'
        . '000d' . '74342d32322d766563746f7273'
        . '00000001'
        . '00000000' . '0054'
        . '002f' . '4c656164657220656c656374696f6e206e6f74206e656564656420666f7220746f70696320706172746974696f6e2e';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new ElectLeadersRequestV0(['t4-22-vectors' => [0]], 30000, ElectionType::PREFERRED, 't4-vectors', 702);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::ELECT_LEADERS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'Kafka 2.2 added the api with the version 0');
        self::assertSame(51, $request->getMessageSize());
        self::assertSame(30000, $request->getTimeoutMs());
    }

    public function testAnAlreadyBuiltTopicEntryIsTakenAsItIs(): void
    {
        $request = new ElectLeadersRequestV0(
            ['t4-22-vectors' => new ElectLeadersRequestTopicPartitions('t4-22-vectors', [0])],
            30000,
            ElectionType::PREFERRED,
            't4-vectors',
            702
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
    }

    public function testANullTopicArrayIsTheCountMinusOneAndTheDefaultOfTheRequest(): void
    {
        $request = new ElectLeadersRequestV0(
            ElectLeadersRequest::ALL_PARTITIONS,
            30000,
            ElectionType::PREFERRED,
            't4-vectors',
            702
        );

        self::assertSame(self::ALL_PARTITIONS_REQUEST_HEX, bin2hex((string) $request));
        self::assertNull($request->getTopicPartitions());
        self::assertNull(ElectLeadersRequest::ALL_PARTITIONS, 'the null array is the documented default');
        self::assertSame(60000, ElectLeadersRequest::DEFAULT_TIMEOUT_MS, 'the 60 seconds of the Java client');
    }

    public function testAnEmptyTopicArrayIsNotTheSameFrameAsANullOne(): void
    {
        $empty = new ElectLeadersRequestV0([], 30000, ElectionType::PREFERRED, 't4-vectors', 702);

        self::assertStringContainsString('00000000' . '00007530', bin2hex((string) $empty));
        self::assertNotSame(self::ALL_PARTITIONS_REQUEST_HEX, bin2hex((string) $empty));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = ElectLeadersResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(702, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['t4-22-vectors'], array_keys($response->replicaElectionResults));

        $partition = $response->replicaElectionResults['t4-22-vectors']->partitionResult[0];
        self::assertSame(0, $partition->partitionId);
        self::assertSame(KafkaException::ELECTION_NOT_NEEDED, $partition->errorCode);
        self::assertSame('Leader election not needed for topic partition.', $partition->errorMessage);
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testTheAnswerOfVersionZeroHasNoTopLevelErrorCode(): void
    {
        // `ElectLeadersResponse(throttleTimeMs, errorCode, results, version)` @ 2.8.2 writes the top-level code
        // only `if (version >= 1)`, so the scheme of this version goes straight from the throttle time to the array
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'replicaElectionResults'],
            array_keys(ElectLeadersResponseV0::getScheme())
        );
    }

    /**
     * ElectLeaders request v1, the frame of KIP-460: an `election_type` byte in front of the topic array.
     *
     *   Size            => 00 00 00 34 (52 bytes)
     *   ApiKey          => 00 2b (43)
     *   ApiVersion      => 00 01
     *   CorrelationId   => 00 00 03 f3 (1011)
     *   ClientId        => 00 0a "t4-vectors"
     *   ElectionType    => 00 (PREFERRED)
     *   TopicPartitions => 00 00 00 01, 00 0d "t4-24-vectors", 00 00 00 01, 00 00 00 00
     *   TimeoutMs       => 00 00 75 30 (30000)
     */
    private const string REQUEST_V1_HEX = '00000034'
        . '002b'
        . '0001'
        . '000003f3'
        . '000a' . '74342d766563746f7273'
        . '00'
        . '00000001'
        . '000d' . '74342d32342d766563746f7273'
        . '00000001' . '00000000'
        . '00007530';

    /**
     * The answer of that request: the top-level error code 0 between the throttle time and the results.
     */
    private const string RESPONSE_V1_HEX = '00000058'
        . '000003f3'
        . '00000000'
        . '0000'
        . '00000001'
        . '000d' . '74342d32342d766563746f7273'
        . '00000001'
        . '00000000' . '0054'
        . '002f' . '4c656164657220656c656374696f6e206e6f74206e656564656420666f7220746f70696320706172746974696f6e2e';

    public function testTheVersionOneCarriesTheElectionTypeOfKip460(): void
    {
        $request = new ElectLeadersRequest(
            ['t4-24-vectors' => [0]],
            30000,
            ElectionType::PREFERRED,
            't4-vectors',
            1011
        );

        self::assertSame(1, $request->getApiVersion(), 'the version this line sends');
        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(ElectionType::PREFERRED, $request->getElectionType());
    }

    public function testTheUncleanElectionIsTheSameFrameWithTheTypeOne(): void
    {
        $request = new ElectLeadersRequest(
            ['t4-24-vectors' => [0]],
            30000,
            ElectionType::UNCLEAN,
            't4-vectors',
            1012
        );

        // The election type is the 25th byte of the frame: 4 size + 2 api key + 2 version + 4 correlation id +
        // 2 + 10 client id, so its hex offset is 48
        self::assertSame(
            substr_replace(str_replace('000003f3', '000003f4', self::REQUEST_V1_HEX), '01', 48, 2),
            bin2hex((string) $request),
            'only the election type byte and the correlation id differ'
        );
        self::assertSame(ElectionType::UNCLEAN, $request->getElectionType());
    }

    public function testTheElectionTypeIsTheFirstFieldOfTheBodyAndTheErrorCodeFollowsTheThrottleTime(): void
    {
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'electionType', 'topicPartitions', 'timeoutMs'],
            array_keys(ElectLeadersRequest::getScheme()),
            'the `election_type` of KIP-460 stands in FRONT of the topic array'
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'errorCode', 'replicaElectionResults'],
            array_keys(ElectLeadersResponse::getScheme()),
            'and the top-level error code between the throttle time and the results'
        );
    }

    public function testTheTopLevelErrorCodeOfVersionOneIsRead(): void
    {
        $response = ElectLeadersResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'everything that reached the controller');
        self::assertSame(
            KafkaException::ELECTION_NOT_NEEDED,
            $response->replicaElectionResults['t4-24-vectors']->partitionResult[0]->errorCode
        );
        self::assertSame(self::RESPONSE_V1_HEX, bin2hex((string) $response));
    }

    public function testTheElectionTypesAreTheOnesOfTheJavaClient(): void
    {
        self::assertSame(0, ElectionType::PREFERRED);
        self::assertSame(1, ElectionType::UNCLEAN);
        self::assertTrue(ElectionType::isKnown(ElectionType::PREFERRED));
        self::assertFalse(ElectionType::isKnown(7));
        self::assertSame('PREFERRED', ElectionType::nameOf(ElectionType::PREFERRED));
        self::assertSame('UNCLEAN', ElectionType::nameOf(ElectionType::UNCLEAN));
        self::assertSame('UNKNOWN(7)', ElectionType::nameOf(7));
    }
}
