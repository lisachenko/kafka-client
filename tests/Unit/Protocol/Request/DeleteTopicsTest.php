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
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponse;

/**
 * Byte-exact tests for the DeleteTopics API of Kafka 0.10.1 (api key 20, v0).
 *
 * @see docs/protocol/0.11.0.md, section "DeleteTopics API (key 20, v0)"
 */
#[CoversClass(DeleteTopicsRequest::class)]
#[CoversClass(DeleteTopicsResponse::class)]
#[CoversClass(DeleteTopicsResponseTopic::class)]
final class DeleteTopicsTest extends TestCase
{
    /**
     * DeleteTopics request v0 for two topics.
     *
     *   Size          => 00 00 00 24 (36 bytes)
     *   ApiKey        => 00 14 (20)
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 03
     *   ClientId      => 00 04 "test"
     *   Topics        => 00 00 00 02
     *     00 05 "topic"
     *     00 05 "other"
     *   Timeout       => 00 00 75 30 (30000)
     */
    private const string REQUEST_HEX = '00000024'
        . '0014'
        . '0000'
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
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 04
     *   ClientId      => 00 04 "test"
     *   Topics        => 00 00 00 01
     *     00 1f "#kafka-client-controller-probe#"
     *   Timeout       => 00 00 00 00
     */
    private const string PROBE_REQUEST_HEX = '00000037'
        . '0014'
        . '0000'
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

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DeleteTopicsRequest(['topic', 'other'], 30000, 'test', 3);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DELETE_TOPICS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'a 0.10.2.2 broker only serves version 0');
        self::assertSame(36, $request->getMessageSize());
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
        $response = DeleteTopicsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(3, $response->getCorrelationId());
        self::assertSame(['topic', 'other'], array_keys($response->topics), 'topics are keyed by their name');
        self::assertSame(KafkaException::NO_ERROR, $response->topics['topic']->errorCode);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $response->topics['other']->errorCode);
    }

    public function testABrokerThatIsNotTheControllerReportsFortyOneForEveryTopic(): void
    {
        $response = DeleteTopicsResponse::unpack(
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
        $response = DeleteTopicsResponse::unpack(new StringStream((string) hex2bin(self::EMPTY_RESPONSE_HEX)));

        self::assertSame([], $response->topics);
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $response = DeleteTopicsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }
}
