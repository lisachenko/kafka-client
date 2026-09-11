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
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\CreateTopicsRequestConfig;
use Protocol\Kafka\Protocol\Data\CreateTopicsRequestReplicaAssignment;
use Protocol\Kafka\Protocol\Data\CreateTopicsRequestTopic;
use Protocol\Kafka\Protocol\Data\CreateTopicsResponseTopic;
use Protocol\Kafka\Protocol\Data\CreateTopicsResponseTopicV0;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequest;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV0;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV1;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV2;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV3;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV4;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponse;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponseV0;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponseV1;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponseV2;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponseV3;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponseV4;

/**
 * Byte-exact tests for the CreateTopics API of Kafka 0.10.1 (api key 19), raised to version 2 by KIP-124
 * and to version 3 by Kafka 2.0 (KIP-219).
 *
 * The request of the versions 1, 2 and 3 is one and the same body - `CREATE_TOPICS_REQUEST_V2 =
 * CREATE_TOPICS_REQUEST_V1` and `CREATE_TOPICS_REQUEST_V3 = CREATE_TOPICS_REQUEST_V2` @ 2.0.1 - and the
 * answer of version 2 and 3 is the answer of version 1 with the leading `ThrottleTimeMs`; the topic
 * entries are the ones of version 1 in every one of them.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v5)"
 */
#[CoversClass(CreateTopicsRequest::class)]
#[CoversClass(CreateTopicsRequestV2::class)]
#[CoversClass(CreateTopicsRequestV3::class)]
#[CoversClass(CreateTopicsRequestV0::class)]
#[CoversClass(CreateTopicsRequestV1::class)]
#[CoversClass(CreateTopicsResponse::class)]
#[CoversClass(CreateTopicsResponseV2::class)]
#[CoversClass(CreateTopicsResponseV3::class)]
#[CoversClass(CreateTopicsResponseV0::class)]
#[CoversClass(CreateTopicsResponseV1::class)]
#[CoversClass(CreateTopicsRequestTopic::class)]
#[CoversClass(CreateTopicsRequestReplicaAssignment::class)]
#[CoversClass(CreateTopicsRequestConfig::class)]
#[CoversClass(CreateTopicsResponseTopic::class)]
#[CoversClass(CreateTopicsResponseTopicV0::class)]
#[CoversClass(NewTopic::class)]
#[CoversClass(CreateTopicsRequestV4::class)]
#[CoversClass(CreateTopicsResponseV4::class)]
final class CreateTopicsTest extends TestCase
{
    /**
     * CreateTopics request v2 that creates `topic` with two partitions and one topic level option.
     *
     *   Size              => 00 00 00 43 (67 bytes)
     *   ApiKey            => 00 13 (19)
     *   ApiVersion        => 00 02
     *   CorrelationId     => 00 00 00 07
     *   ClientId          => 00 04 "test"
     *   Topics            => 00 00 00 01
     *     Topic             => 00 05 "topic"
     *     NumPartitions     => 00 00 00 02
     *     ReplicationFactor => 00 01
     *     ReplicaAssignment => 00 00 00 00
     *     Configs           => 00 00 00 01
     *       ConfigKey   => 00 0c "retention.ms"
     *       ConfigValue => 00 07 "3600000"
     *   Timeout           => 00 00 75 30 (30000)
     *   ValidateOnly      => 00
     */
    private const string REQUEST_V2_HEX = '00000043'
        . '0013'
        . '0002'
        . '00000007'
        . '0004' . '74657374'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '0001'
        . '00000000'
        . '00000001'
        . '000c' . '726574656e74696f6e2e6d73'
        . '0007' . '33363030303030'
        . '00007530'
        . '00';

    /**
     * The same topic described by an explicit replica assignment instead of partitions and a replication factor.
     *
     *   Size              => 00 00 00 40 (64 bytes)
     *   ApiKey            => 00 13 (19)
     *   ApiVersion        => 00 02
     *   CorrelationId     => 00 00 00 08
     *   ClientId          => 00 04 "test"
     *   Topics            => 00 00 00 01
     *     Topic             => 00 05 "topic"
     *     NumPartitions     => ff ff ff ff (-1, unset)
     *     ReplicationFactor => ff ff (-1, unset)
     *     ReplicaAssignment => 00 00 00 01
     *       PartitionId => 00 00 00 00
     *       Replicas    => 00 00 00 03, then 00 00 00 02, 00 00 00 01, 00 00 00 02
     *     Configs           => 00 00 00 00
     *   Timeout           => 00 00 75 30 (30000)
     *   ValidateOnly      => 01
     */
    private const string REQUEST_V2_ASSIGNMENT_HEX = '00000040'
        . '0013'
        . '0002'
        . '00000008'
        . '0004' . '74657374'
        . '00000001'
        . '0005' . '746f706963'
        . 'ffffffff'
        . 'ffff'
        . '00000001'
        . '00000000'
        . '00000003' . '00000002' . '00000001' . '00000002'
        . '00000000'
        . '00007530'
        . '01';

    /**
     * The same request as version 0, i.e. without the trailing `ValidateOnly` byte that version 1 added.
     */
    private const string REQUEST_V0_HEX = '00000042'
        . '0013'
        . '0000'
        . '00000007'
        . '0004' . '74657374'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '0001'
        . '00000000'
        . '00000001'
        . '000c' . '726574656e74696f6e2e6d73'
        . '0007' . '33363030303030'
        . '00007530';

    /**
     * CreateTopics response v1 for two topics: one created, one that already exists.
     *
     *   Size          => 00 00 00 33 (51 bytes)
     *   CorrelationId => 00 00 00 07
     *   TopicErrors   => 00 00 00 02
     *     Topic        => 00 05 "topic"
     *     ErrorCode    => 00 00
     *     ErrorMessage => ff ff (null)
     *     Topic        => 00 05 "other"
     *     ErrorCode    => 00 24 (36)
     *     ErrorMessage => 00 15 "Topic 'other' exists."
     */
    private const string RESPONSE_V1_HEX = '00000033'
        . '00000007'
        . '00000002'
        . '0005' . '746f706963'
        . '0000'
        . 'ffff'
        . '0005' . '6f74686572'
        . '0024'
        . '0015' . '546f70696320276f7468657227206578697374732e';

    /**
     * CreateTopics response v0 for the same two topics, without the error messages of version 1.
     *
     *   Size          => 00 00 00 1a (26 bytes)
     *   CorrelationId => 00 00 00 07
     *   TopicErrors   => 00 00 00 02
     *     Topic     => 00 05 "topic", ErrorCode => 00 00
     *     Topic     => 00 05 "other", ErrorCode => 00 24 (36)
     */
    private const string RESPONSE_V0_HEX = '0000001a'
        . '00000007'
        . '00000002'
        . '0005' . '746f706963' . '0000'
        . '0005' . '6f74686572' . '0024';

    /**
     * The same answer as version 2: the topic errors of version 1 behind the throttle time KIP-124 added.
     *
     *   Size           => 00 00 00 37 (55 bytes)
     *   CorrelationId  => 00 00 00 07
     *   ThrottleTimeMs => 00 00 00 00
     *   TopicErrors    => (the entries of version 1)
     */
    private const string RESPONSE_V2_HEX = '00000037'
        . '00000007'
        . '00000000'
        . '00000002'
        . '0005' . '746f706963'
        . '0000'
        . 'ffff'
        . '0005' . '6f74686572'
        . '0024'
        . '0015' . '546f70696320276f7468657227206578697374732e';

    public function testRequestOfVersionTwoIsPackedAccordingToTheSpec(): void
    {
        $request = new CreateTopicsRequestV2(
            [new NewTopic('topic', 2, 1, configs: ['retention.ms' => '3600000'])],
            30000,
            false,
            'test',
            7
        );

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::CREATE_TOPICS, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion());
    }

    public function testTheClientSendsTheVersionFourOfKafkaTwoFour(): void
    {
        $request = new CreateTopicsRequestV4(
            [new NewTopic('topic', 2, 1, configs: ['retention.ms' => '3600000'])],
            30000,
            false,
            'test',
            7
        );

        // The layout of `CreateTopicsRequest.json` @ 2.8.2 is the same for the versions 1 to 4: Kafka 2.0 raised
        // the api for KIP-219 and Kafka 2.4 for KIP-464, and neither of them touched a byte of the frame
        self::assertSame(4, $request->getApiVersion());
        self::assertSame(substr_replace(self::REQUEST_V2_HEX, '0004', 12, 4), bin2hex((string) $request));

        $answer = CreateTopicsResponseV4::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(
            self::RESPONSE_V2_HEX,
            bin2hex((string) $answer),
            'the answer of version 4 has the layout of version 2 as well'
        );
    }

    public function testTheVersionThreeOfKafkaTwoZeroSendsTheSameBytes(): void
    {
        $request = new CreateTopicsRequestV3(
            [new NewTopic('topic', 2, 1, configs: ['retention.ms' => '3600000'])],
            30000,
            false,
            'test',
            7
        );

        // `CREATE_TOPICS_REQUEST_V3 = CREATE_TOPICS_REQUEST_V2` @ 2.0.1: only the api version field is different
        self::assertSame(3, $request->getApiVersion());
        self::assertSame(substr_replace(self::REQUEST_V2_HEX, '0003', 12, 4), bin2hex((string) $request));

        $answer = CreateTopicsResponseV3::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(self::RESPONSE_V2_HEX, bin2hex((string) $answer));
    }

    /**
     * The -1/-1 of KIP-464: a topic whose partition count and replication factor the BROKER chooses
     */
    public function testTheBrokerDefaultsOfKip464TravelAsMinusOneWithoutAnAssignment(): void
    {
        $topic = NewTopic::withBrokerDefaults('topic', ['retention.ms' => '3600000']);

        self::assertSame(NewTopic::NO_NUM_PARTITIONS, $topic->numPartitions);
        self::assertSame(NewTopic::NO_REPLICATION_FACTOR, $topic->replicationFactor);
        self::assertSame([], $topic->replicasAssignments, 'and no assignment, which is what makes it KIP-464');

        $request = new CreateTopicsRequestV4([$topic], 30000, false, 'test', 7);
        $hex     = bin2hex((string) $request);

        // topic "topic", num_partitions = -1, replication_factor = -1, an empty assignment array
        self::assertStringContainsString(
            '0005' . '746f706963' . 'ffffffff' . 'ffff' . '00000000',
            $hex,
            'both numbers are -1 and the assignment array is empty'
        );
        self::assertSame(4, $request->getApiVersion(), 'the version a broker accepts that shape from');
    }

    /**
     * The very guard of `CreateTopicsRequest.Builder.build(version)` @ 2.8.2 - the broker has none
     */
    public function testTheBrokerDefaultsAreRefusedByEveryVersionBelowFour(): void
    {
        try {
            new CreateTopicsRequestV3([NewTopic::withBrokerDefaults('topic')], 30000, false, 'test', 7);
            self::fail('a version below 4 cannot carry the broker defaults of KIP-464');
        } catch (UnsupportedVersionException $exception) {
            self::assertSame('topic', $exception->getContext()['topics']);
            self::assertStringContainsString(
                'only supported in CreateTopicRequest version 4+',
                (string) $exception->getContext()['error'],
                'the message of the Java client'
            );
        }
    }

    /**
     * A topic that names its replicas is not "using the defaults", whatever the -1 of the two numbers says
     */
    public function testAnExplicitAssignmentKeepsTheMinusOneLegalInEveryVersion(): void
    {
        $request = new CreateTopicsRequestV0(
            [NewTopic::withReplicaAssignment('topic', [0 => [0]])],
            30000,
            'test',
            7
        );

        self::assertSame(0, $request->getApiVersion(), 'the -1 of an assigned topic is the one of Kafka 0.10');
    }

    public function testRequestOfVersionOneSendsTheSameBodyAsVersionTwo(): void
    {
        $request = new CreateTopicsRequestV1(
            [new NewTopic('topic', 2, 1, configs: ['retention.ms' => '3600000'])],
            30000,
            false,
            'test',
            7
        );

        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr_replace(self::REQUEST_V2_HEX, '0001', 12, 4),
            bin2hex((string) $request),
            'CREATE_TOPICS_REQUEST_V2 = CREATE_TOPICS_REQUEST_V1'
        );
    }

    public function testAnExplicitReplicaAssignmentLeavesThePartitionsAndTheFactorUnset(): void
    {
        $request = new CreateTopicsRequestV2(
            [NewTopic::withReplicaAssignment('topic', [0 => [2, 1, 2]])],
            30000,
            true,
            'test',
            8
        );

        self::assertSame(self::REQUEST_V2_ASSIGNMENT_HEX, bin2hex((string) $request));
        self::assertSame(-1, NewTopic::NO_NUM_PARTITIONS, 'unset is -1 on the wire');
        self::assertSame(-1, NewTopic::NO_REPLICATION_FACTOR);
    }

    public function testRequestOfVersionZeroHasNoValidateOnlyFlag(): void
    {
        $request = new CreateTopicsRequestV0(
            [new NewTopic('topic', 2, 1, configs: ['retention.ms' => '3600000'])],
            30000,
            'test',
            7
        );

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(
            strlen((string) hex2bin(self::REQUEST_V2_HEX)) - 1,
            strlen((string) $request),
            'the boolean of version 1 is exactly one byte'
        );
    }

    public function testAnAlreadyBuiltTopicEntryIsAcceptedAsItIs(): void
    {
        $request = new CreateTopicsRequestV2(
            [new CreateTopicsRequestTopic('topic', 2, 1, [], ['retention.ms' => '3600000'])],
            30000,
            false,
            'test',
            7
        );

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request));
    }

    public function testResponseOfVersionOneIsUnpackedAccordingToTheSpec(): void
    {
        $response = CreateTopicsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(['topic', 'other'], array_keys($response->topics), 'topics are keyed by their name');

        self::assertSame(KafkaException::NO_ERROR, $response->topics['topic']->errorCode);
        self::assertNull($response->topics['topic']->errorMessage, 'ff ff is the null of a nullable string');

        self::assertSame(KafkaException::TOPIC_ALREADY_EXISTS, $response->topics['other']->errorCode);
        self::assertSame("Topic 'other' exists.", $response->topics['other']->errorMessage);
    }

    public function testResponseOfVersionZeroCarriesNoErrorMessage(): void
    {
        $response = CreateTopicsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_V0_HEX)));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(['topic', 'other'], array_keys($response->topics));
        self::assertSame(KafkaException::NO_ERROR, $response->topics['topic']->errorCode);
        self::assertSame(KafkaException::TOPIC_ALREADY_EXISTS, $response->topics['other']->errorCode);
        foreach ($response->topics as $topic) {
            self::assertInstanceOf(CreateTopicsResponseTopicV0::class, $topic);
            self::assertNull($topic->errorMessage, 'version 0 never carries a message');
        }
    }

    public function testResponseOfVersionTwoStartsWithTheThrottleTime(): void
    {
        $response = CreateTopicsResponseV4::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['topic', 'other'], array_keys($response->topics));
        self::assertSame("Topic 'other' exists.", $response->topics['other']->errorMessage);
        self::assertSame(self::RESPONSE_V2_HEX, bin2hex((string) $response));
    }

    public function testEveryVersionOfTheResponseSurvivesARoundTrip(): void
    {
        $versionZero = CreateTopicsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_V0_HEX)));
        $versionOne  = CreateTopicsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));
        $versionTwo  = CreateTopicsResponseV4::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(self::RESPONSE_V0_HEX, bin2hex((string) $versionZero));
        self::assertSame(self::RESPONSE_V1_HEX, bin2hex((string) $versionOne));
        self::assertSame(self::RESPONSE_V2_HEX, bin2hex((string) $versionTwo));
    }

    public function testTheTopicsOfTheRequestAreDeduplicatedByTheirName(): void
    {
        // The wire format is an array, so a broker has to guard against duplicates itself and answers them with the
        // error code 42; this client can not produce such a frame, because it keys its topics by name
        $request = new CreateTopicsRequestV2(
            [new NewTopic('topic', 1, 1), new NewTopic('topic', 2, 1, configs: ['retention.ms' => '3600000'])],
            30000,
            false,
            'test',
            7
        );

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request), 'the last entry of a name wins');
    }
}
