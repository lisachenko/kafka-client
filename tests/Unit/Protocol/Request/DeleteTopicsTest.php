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
use Protocol\Kafka\Protocol\Data\DeleteTopicsResponseTopic;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequest;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequestV0;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequestV1;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequestV2;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponse;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponseV0;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponseV1;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponseV2;

/**
 * Byte-exact tests for the DeleteTopics API of Kafka 0.10.1 (api key 20), raised to version 1 by KIP-124 and
 * to version 2 by Kafka 2.0.
 *
 * The request of the four versions is one and the same body - `DELETE_TOPICS_REQUEST_V1 =
 * DELETE_TOPICS_REQUEST_V0`, `DELETE_TOPICS_REQUEST_V2 = DELETE_TOPICS_REQUEST_V1` and
 * `DELETE_TOPICS_REQUEST_V3 = DELETE_TOPICS_REQUEST_V2` - and so is the answer of the versions 1 to 3; only the
 * answer of version 0 has no `ThrottleTimeMs` in front of the array. What the versions buy is an error code: the
 * throttling promise of KIP-219 at version 2, and the **73** `TOPIC_DELETION_DISABLED` of a cluster with
 * `delete.topic.enable=false` at version 3, which a version 2 client is answered with 42 for.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v3)"
 */
#[CoversClass(DeleteTopicsRequest::class)]
#[CoversClass(DeleteTopicsRequestV1::class)]
#[CoversClass(DeleteTopicsRequestV2::class)]
#[CoversClass(DeleteTopicsRequestV0::class)]
#[CoversClass(DeleteTopicsResponse::class)]
#[CoversClass(DeleteTopicsResponseV1::class)]
#[CoversClass(DeleteTopicsResponseV2::class)]
#[CoversClass(DeleteTopicsResponseV0::class)]
#[CoversClass(DeleteTopicsResponseTopic::class)]
final class DeleteTopicsTest extends TestCase
{
    /**
     * DeleteTopics request v3 for two topics.
     *
     *   Size          => 00 00 00 24 (36 bytes)
     *   ApiKey        => 00 14 (20)
     *   ApiVersion    => 00 03
     *   CorrelationId => 00 00 00 03
     *   ClientId      => 00 04 "test"
     *   Topics        => 00 00 00 02
     *     00 05 "topic"
     *     00 05 "other"
     *   Timeout       => 00 00 75 30 (30000)
     */
    private const string REQUEST_HEX = '00000024'
        . '0014'
        . '0003'
        . '00000003'
        . '0004' . '74657374'
        . '00000002'
        . '0005' . '746f706963'
        . '0005' . '6f74686572'
        . '00007530';

    /**
     * The controller probe of `AdminClient::findController()`: one topic name that can never be legal, timeout 0.
     *
     *   Size          => 00 00 00 37 (55 bytes)
     *   ApiKey        => 00 14 (20)
     *   ApiVersion    => 00 03
     *   CorrelationId => 00 00 00 04
     *   ClientId      => 00 04 "test"
     *   Topics        => 00 00 00 01
     *     00 1f "#kafka-client-controller-probe#"
     *   Timeout       => 00 00 00 00
     */
    private const string PROBE_REQUEST_HEX = '00000037'
        . '0014'
        . '0003'
        . '00000004'
        . '0004' . '74657374'
        . '00000001'
        . '001f' . '236b61666b612d636c69656e742d636f6e74726f6c6c65722d70726f626523'
        . '00000000';

    /**
     * DeleteTopics response v0: one topic deleted, one the cluster does not have.
     *
     *   Size            => 00 00 00 1a (26 bytes)
     *   CorrelationId   => 00 00 00 03
     *   TopicErrorCodes => 00 00 00 02
     *     Topic => 00 05 "topic", ErrorCode => 00 00
     *     Topic => 00 05 "other", ErrorCode => 00 03
     */
    private const string RESPONSE_HEX = '0000001a'
        . '00000003'
        . '00000002'
        . '0005' . '746f706963' . '0000'
        . '0005' . '6f74686572' . '0003';

    /**
     * The answer of a broker that is not the active controller: the error code 41 for every requested topic.
     */
    private const string NOT_CONTROLLER_RESPONSE_HEX = '0000001a'
        . '00000003'
        . '00000002'
        . '0005' . '746f706963' . '0029'
        . '0005' . '6f74686572' . '0029';

    /**
     * The answer to a request with an empty topic array, which every broker of the cluster gives.
     */
    private const string EMPTY_RESPONSE_HEX = '00000008' . '00000003' . '00000000';

    /**
     * The same answer as version 1: the topic error codes behind the throttle time KIP-124 added.
     *
     *   Size            => 00 00 00 1e (30 bytes)
     *   CorrelationId   => 00 00 00 03
     *   ThrottleTimeMs  => 00 00 00 00
     *   TopicErrorCodes => 00 00 00 02
     */
    private const string RESPONSE_V1_HEX = '0000001e'
        . '00000003'
        . '00000000'
        . '00000002'
        . '0005' . '746f706963' . '0000'
        . '0005' . '6f74686572' . '0003';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DeleteTopicsRequest(['topic', 'other'], 30000, 'test', 3);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DELETE_TOPICS, $request->getApiKey());
        self::assertSame(3, $request->getApiVersion(), 'Kafka 2.1 raised the api to version 3 (the 73 of KIP-412)');
        self::assertSame(36, $request->getMessageSize());
    }

    public function testTheLowerVersionsAreTheSameBodyWithALowerVersionField(): void
    {
        $versionZero = new DeleteTopicsRequestV0(['topic', 'other'], 30000, 'test', 3);
        $versionOne  = new DeleteTopicsRequestV1(['topic', 'other'], 30000, 'test', 3);
        $versionTwo  = new DeleteTopicsRequestV2(['topic', 'other'], 30000, 'test', 3);

        self::assertSame(0, $versionZero->getApiVersion());
        self::assertSame(1, $versionOne->getApiVersion());
        self::assertSame(2, $versionTwo->getApiVersion());
        self::assertSame(
            substr_replace(self::REQUEST_HEX, '0002', 12, 4),
            bin2hex((string) $versionTwo),
            'DELETE_TOPICS_REQUEST_V3 = DELETE_TOPICS_REQUEST_V2'
        );
        self::assertSame(
            substr_replace(self::REQUEST_HEX, '0000', 12, 4),
            bin2hex((string) $versionZero),
            'DELETE_TOPICS_REQUEST_V1 = DELETE_TOPICS_REQUEST_V0'
        );
        self::assertSame(
            substr_replace(self::REQUEST_HEX, '0001', 12, 4),
            bin2hex((string) $versionOne),
            'DELETE_TOPICS_REQUEST_V2 = DELETE_TOPICS_REQUEST_V1'
        );
    }

    public function testTheAnswerOfVersionOneIsReadByTheClassOfItsOwnVersion(): void
    {
        $response = DeleteTopicsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));
        $twoAgain = DeleteTopicsResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertSame(self::RESPONSE_V1_HEX, bin2hex((string) $response));
        self::assertSame(self::RESPONSE_V1_HEX, bin2hex((string) $twoAgain));
        self::assertSame(
            array_keys(DeleteTopicsResponse::getScheme()),
            array_keys(DeleteTopicsResponseV1::getScheme()),
            'the answers of the versions 1, 2 and 3 have one and the same layout'
        );
    }

    public function testTheTopicsAreAPlainStringArrayAndNotAStructure(): void
    {
        // The array holds the names themselves, so the frame of two topics is exactly two length-prefixed strings
        $one = new DeleteTopicsRequest(['topic'], 30000, 'test', 3);
        $two = new DeleteTopicsRequest(['topic', 'other'], 30000, 'test', 3);

        self::assertSame(strlen((string) $one) + 7, strlen((string) $two), '00 05 plus five characters');
    }

    public function testTheControllerProbeNamesATopicThatCanNotExist(): void
    {
        $probe = new DeleteTopicsRequest(['#kafka-client-controller-probe#'], 0, 'test', 4);

        self::assertSame(self::PROBE_REQUEST_HEX, bin2hex((string) $probe));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DeleteTopicsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(3, $response->getCorrelationId());
        self::assertSame(['topic', 'other'], array_keys($response->topics), 'topics are keyed by their name');
        self::assertSame(0, $response->throttleTimeMs, 'version 0 has no throttle time');
        self::assertSame(KafkaException::NO_ERROR, $response->topics['topic']->errorCode);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $response->topics['other']->errorCode);
    }

    public function testABrokerThatIsNotTheControllerReportsFortyOneForEveryTopic(): void
    {
        $response = DeleteTopicsResponseV0::unpack(
            new StringStream((string) hex2bin(self::NOT_CONTROLLER_RESPONSE_HEX))
        );

        foreach ($response->topics as $topic) {
            self::assertSame(KafkaException::NOT_CONTROLLER, $topic->errorCode);
        }
    }

    public function testAnEmptyTopicArrayIsAnsweredWithAnEmptyResult(): void
    {
        // Which is why findController() can not probe with an empty request: a follower answers it exactly like the
        // controller does, see KafkaApis.handleDeleteTopicsRequest @ 0.10.2.2
        $response = DeleteTopicsResponseV0::unpack(new StringStream((string) hex2bin(self::EMPTY_RESPONSE_HEX)));

        self::assertSame([], $response->topics);
    }

    public function testTheVersionOneAnswerStartsWithTheThrottleTime(): void
    {
        $response = DeleteTopicsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertSame(3, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['topic', 'other'], array_keys($response->topics));
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $response->topics['other']->errorCode);
        self::assertSame(self::RESPONSE_V1_HEX, bin2hex((string) $response));
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $response = DeleteTopicsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }
}
