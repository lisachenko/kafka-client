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
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\InitProducerIdResponseV2;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequestV2;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequest;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequestV0;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequestV1;
use Protocol\Kafka\Protocol\Request\InitProducerIdResponse;
use Protocol\Kafka\Protocol\Request\InitProducerIdResponseV0;
use Protocol\Kafka\Protocol\Request\InitProducerIdResponseV1;

/**
 * Byte-exact tests for the InitProducerId API of Kafka 0.11 (api key 22, v0).
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v3)"
 */
#[CoversClass(InitProducerIdRequest::class)]
#[CoversClass(InitProducerIdRequestV0::class)]
#[CoversClass(InitProducerIdResponse::class)]
#[CoversClass(InitProducerIdResponseV0::class)]
#[CoversClass(InitProducerIdRequestV2::class)]
#[CoversClass(InitProducerIdResponseV2::class)]
final class InitProducerIdTest extends TestCase
{
    /**
     * InitProducerId request v0 of an idempotent producer, i.e. without a transactional id.
     *
     *   Size                 => 00 00 00 14 (20 bytes)
     *   ApiKey               => 00 16 (22)
     *   ApiVersion           => 00 01
     *   CorrelationId        => 00 00 00 07
     *   ClientId             => 00 04 "test"
     *   TransactionalId      => ff ff (null)
     *   TransactionTimeoutMs => 00 00 ea 60 (60000)
     */
    private const string REQUEST_HEX = '00000014'
        . '0016'
        . '0001'
        . '00000007'
        . '0004' . '74657374'
        . 'ffff'
        . '0000ea60';

    /**
     * The same request of a transactional producer: the id is a real string, the timeout a smaller one.
     *
     *   Size                 => 00 00 00 19 (25 bytes)
     *   TransactionalId      => 00 05 "tx-42"
     *   TransactionTimeoutMs => 00 00 75 30 (30000)
     */
    private const string TRANSACTIONAL_REQUEST_HEX = '00000019'
        . '0016'
        . '0001'
        . '00000008'
        . '0004' . '74657374'
        . '0005' . '74782d3432'
        . '00007530';

    /**
     * InitProducerId response v0 with a producer id.
     *
     *   Size           => 00 00 00 14 (20 bytes)
     *   CorrelationId  => 00 00 00 07
     *   ThrottleTimeMs => 00 00 00 00
     *   ErrorCode      => 00 00
     *   ProducerId     => 00 00 00 00 00 00 07 d0 (2000)
     *   ProducerEpoch  => 00 03
     */
    private const string RESPONSE_HEX = '00000014'
        . '00000007'
        . '00000000'
        . '0000'
        . '00000000000007d0'
        . '0003';

    /**
     * The answer to a transaction timeout the broker refuses: the error code 50 and -1 / -1 as the producer state.
     */
    private const string ERROR_RESPONSE_HEX = '00000014'
        . '00000009'
        . '00000000'
        . '0032'
        . 'ffffffffffffffff'
        . 'ffff';

    /**
     * The same request as the **flexible** version 2 of Kafka 2.4, which is what this client sends.
     *
     *   Size                 => 00 00 00 15 (21 bytes)
     *   ApiVersion           => 00 02
     *   ClientId             => 00 04 "test"   (int16 length even here)
     *   TAG_BUFFER           => 00             (of the request header v2)
     *   TransactionalId      => 00             (the compact null, one byte instead of two)
     *   TransactionTimeoutMs => 00 00 ea 60
     *   TAG_BUFFER           => 00             (of the body)
     */
    private const string REQUEST_V2_HEX = '00000015'
        . '0016'
        . '0002'
        . '00000007'
        . '0004' . '74657374'
        . '00'
        . '00'
        . '0000ea60'
        . '00';

    /**
     * The version 2 answer: the same four fields between the tag buffer of the response header v1 and the one of
     * the body - not one of them is a string or an array, so the compact encoding costs two bytes and saves none.
     */
    private const string RESPONSE_V2_HEX = '00000016'
        . '00000007'
        . '00'
        . '00000000'
        . '0000'
        . '00000000000007d0'
        . '0003'
        . '00';

    public function testTheRequestOfAnIdempotentProducerCarriesANullTransactionalId(): void
    {
        $request = new InitProducerIdRequestV1(null, 60000, 'test', 7);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::INIT_PRODUCER_ID, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised the api to version 1 (KIP-219)');
        self::assertNull($request->getTransactionalId());
        self::assertSame(60000, $request->getTransactionTimeoutMs());
    }

    /**
     * Version 2 is the same two fields in the flexible encoding of Kafka 2.4, and it is what the client sends
     */
    public function testTheRequestOfVersionTwoIsCompact(): void
    {
        $request = new InitProducerIdRequestV2(null, 60000, 'test', 7);

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion(), 'Kafka 2.4 raised the api to the flexible version 2');
        self::assertTrue(InitProducerIdRequestV2::isFlexible());
        self::assertNull($request->getTransactionalId(), 'the compact null of a string is the single byte 00');

        $response = InitProducerIdResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(2000, $response->producerId);
        self::assertSame(3, $response->producerEpoch);
        self::assertSame(self::RESPONSE_V2_HEX, bin2hex((string) $response));
    }

    public function testTheTransactionTimeoutIsTheJavaDefaultOfOneMinute(): void
    {
        self::assertSame(60000, InitProducerIdRequest::DEFAULT_TRANSACTION_TIMEOUT_MS);
        self::assertSame(
            InitProducerIdRequest::DEFAULT_TRANSACTION_TIMEOUT_MS,
            new InitProducerIdRequestV2()->getTransactionTimeoutMs()
        );
    }

    public function testTheRequestOfATransactionalProducerCarriesItsId(): void
    {
        $request = new InitProducerIdRequestV1('tx-42', 30000, 'test', 8);

        self::assertSame(self::TRANSACTIONAL_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame('tx-42', $request->getTransactionalId());
    }

    public function testTheRequestIsReadBackFieldByField(): void
    {
        $request = InitProducerIdRequestV1::unpack(new StringStream((string) hex2bin(self::TRANSACTIONAL_REQUEST_HEX)));

        self::assertSame('tx-42', $request->getTransactionalId());
        self::assertSame(30000, $request->getTransactionTimeoutMs());
        self::assertSame(self::TRANSACTIONAL_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testTheAnswerCarriesTheProducerIdAndTheEpoch(): void
    {
        $response = InitProducerIdResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(2000, $response->producerId);
        self::assertSame(3, $response->producerEpoch);
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testAnAnswerWithAnErrorCarriesNoProducerStateAtAll(): void
    {
        $response = InitProducerIdResponseV1::unpack(new StringStream((string) hex2bin(self::ERROR_RESPONSE_HEX)));

        self::assertSame(KafkaException::INVALID_TRANSACTION_TIMEOUT, $response->errorCode);
        self::assertSame(RecordBatch::NO_PRODUCER_ID, $response->producerId);
        self::assertSame(RecordBatch::NO_PRODUCER_EPOCH, $response->producerEpoch);
        self::assertSame(self::ERROR_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testTheEmptyStringIsATransactionalIdAndNotANullOne(): void
    {
        // The broker answers it with the error code 42, but only because it is a real, empty string on the wire
        $frame = bin2hex((string) new InitProducerIdRequestV1('', 60000, 'test', 7));

        self::assertStringContainsString('0000' . '0000ea60', $frame);
        self::assertStringNotContainsString('ffff' . '0000ea60', $frame);

        // and in the flexible version 2 the empty string is the compact length 1, where null is the length 0
        $flexible = bin2hex((string) new InitProducerIdRequestV2('', 60000, 'test', 7));

        self::assertStringContainsString('01' . '0000ea60', $flexible);
        self::assertStringNotContainsString('00' . '0000ea60' . '00' . '0000ea60', $flexible);
    }

    public function testTheVersionZeroFrameIsTheSameBodyWithALowerVersionField(): void
    {
        $request = new InitProducerIdRequestV0(null, 60000, 'test', 7);

        // `INIT_PRODUCER_ID_REQUEST_V1 = INIT_PRODUCER_ID_REQUEST_V0` @ 2.0.1
        self::assertSame(substr_replace(self::REQUEST_HEX, '0000', 12, 4), bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());

        $response = InitProducerIdResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

}
