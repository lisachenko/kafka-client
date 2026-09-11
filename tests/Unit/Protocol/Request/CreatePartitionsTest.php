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

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\NewPartitions;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\CreatePartitionsRequestTopic;
use Protocol\Kafka\Protocol\Data\CreatePartitionsResponseTopic;
use Protocol\Kafka\Protocol\Request\CreatePartitionsRequest;
use Protocol\Kafka\Protocol\Request\CreatePartitionsRequestV0;
use Protocol\Kafka\Protocol\Request\CreatePartitionsRequestV1;
use Protocol\Kafka\Protocol\Request\CreatePartitionsResponse;
use Protocol\Kafka\Protocol\Request\CreatePartitionsResponseV0;
use Protocol\Kafka\Protocol\Request\CreatePartitionsResponseV1;

/**
 * Byte-exact tests for the CreatePartitions API of Kafka 1.0 (api key 37, v0, KIP-195).
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 to v2)"
 */
#[CoversClass(CreatePartitionsRequest::class)]
#[CoversClass(CreatePartitionsRequestV0::class)]
#[CoversClass(CreatePartitionsResponse::class)]
#[CoversClass(CreatePartitionsResponseV0::class)]
#[CoversClass(CreatePartitionsRequestTopic::class)]
#[CoversClass(CreatePartitionsResponseTopic::class)]
#[CoversClass(NewPartitions::class)]
#[CoversClass(CreatePartitionsRequestV1::class)]
#[CoversClass(CreatePartitionsResponseV1::class)]
final class CreatePartitionsTest extends TestCase
{
    /**
     * CreatePartitions request v1 for two topics, one of them with an explicit assignment.
     *
     *   Size          => 00 00 00 49 (73 bytes)
     *   ApiKey        => 00 25 (37)
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 07
     *   ClientId      => 00 04 "test"
     *   TopicPartitions => 00 00 00 02
     *     Topic => 00 04 "grow",   Count => 00 00 00 05, Assignment => ff ff ff ff (null)
     *     Topic => 00 06 "placed", Count => 00 00 00 03, Assignment => 00 00 00 02
     *       00 00 00 01, 00 00 00 00          -- the first added partition, on the broker 0
     *       00 00 00 02, 00 00 00 00, 00 00 00 01 -- the second one, on the brokers 0 and 1
     *   Timeout      => 00 00 75 30 (30000)
     *   ValidateOnly => 00
     */
    private const string REQUEST_HEX = '00000049'
        . '0025'
        . '0001'
        . '00000007'
        . '0004' . '74657374'
        . '00000002'
        . '0004' . '67726f77' . '00000005' . 'ffffffff'
        . '0006' . '706c61636564' . '00000003' . '00000002'
        . '00000001' . '00000000'
        . '00000002' . '00000000' . '00000001'
        . '00007530'
        . '00';

    /**
     * CreatePartitions response v0: one topic that grew and one that was refused with 37.
     *
     *   Size           => 00 00 00 40 (64 bytes)
     *   CorrelationId  => 00 00 00 07
     *   ThrottleTimeMs => 00 00 00 00
     *   TopicErrors    => 00 00 00 02
     *     00 04 "grow",  ErrorCode => 00 00, ErrorMessage => ff ff
     *     00 05 "small", ErrorCode => 00 25 (37), ErrorMessage => 00 1f "Topic already has 3 partitions."
     */
    private const string RESPONSE_HEX = '00000040'
        . '00000007'
        . '00000000'
        . '00000002'
        . '0004' . '67726f77' . '0000' . 'ffff'
        . '0005' . '736d616c6c' . '0025' . '001f'
        . '546f70696320616c726561647920686173203320706172746974696f6e732e';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new CreatePartitionsRequestV1(
            [
                'grow'   => NewPartitions::increaseTo(5),
                'placed' => NewPartitions::increaseTo(3, [[0], [0, 1]]),
            ],
            30000,
            false,
            'test',
            7
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::CREATE_PARTITIONS, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised the api to version 1 (KIP-219)');
    }

    public function testAPlainCountIsTheSameFrameAsANewPartitionsWithoutAnAssignment(): void
    {
        $fromInteger = new CreatePartitionsRequestV1(['grow' => 5], 30000, false, 'test', 7);
        $fromObject  = new CreatePartitionsRequestV1(
            ['grow' => NewPartitions::increaseTo(5)],
            30000,
            false,
            'test',
            7
        );
        $fromEntry   = new CreatePartitionsRequestV1(
            ['grow' => new CreatePartitionsRequestTopic('grow', 5)],
            30000,
            false,
            'test',
            7
        );

        self::assertSame((string) $fromObject, (string) $fromInteger);
        self::assertSame((string) $fromObject, (string) $fromEntry);
        self::assertStringEndsWith('ffffffff' . '00007530' . '00', bin2hex((string) $fromInteger));
    }

    public function testAnAssignmentThatNamesNoBrokerIsRefusedByTheClient(): void
    {
        // The broker would answer 39 for it; there is no reason to send the frame at all
        $this->expectException(InvalidArgumentException::class);

        NewPartitions::increaseTo(3, [[0], []]);
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = CreatePartitionsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['grow', 'small'], array_keys($response->topics), 'the results are keyed by the topic');

        self::assertSame(KafkaException::NO_ERROR, $response->topics['grow']->errorCode);
        self::assertNull($response->topics['grow']->errorMessage);

        self::assertSame(KafkaException::INVALID_PARTITIONS, $response->topics['small']->errorCode);
        self::assertSame(
            'Topic already has 3 partitions.',
            $response->topics['small']->errorMessage,
            'the message of the controller is the only place that says why the count was refused'
        );
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $response = CreatePartitionsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testRequestSurvivesARoundTrip(): void
    {
        $request = CreatePartitionsRequestV1::unpack(new StringStream((string) hex2bin(self::REQUEST_HEX)));

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
    }

    public function testTheVersionZeroFrameIsTheSameBodyWithALowerVersionField(): void
    {
        $request = new CreatePartitionsRequestV0(
            [
                'grow'   => NewPartitions::increaseTo(5),
                'placed' => NewPartitions::increaseTo(3, [[0], [0, 1]]),
            ],
            30000,
            false,
            'test',
            7
        );

        // `CREATE_PARTITIONS_REQUEST_V1 = CREATE_PARTITIONS_REQUEST_V0` @ 2.0.1
        self::assertSame(substr_replace(self::REQUEST_HEX, '0000', 12, 4), bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());

        $response = CreatePartitionsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

}
