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
use Protocol\Kafka\Protocol\Request\InitProducerIdRequest;
use Protocol\Kafka\Protocol\Request\InitProducerIdResponse;

/**
 * Byte-exact tests for the InitProducerId API of Kafka 0.11 (api key 22, v0).
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0)"
 */
#[CoversClass(InitProducerIdRequest::class)]
#[CoversClass(InitProducerIdResponse::class)]
final class InitProducerIdTest extends TestCase
{
    /**
     * InitProducerId request v0 of an idempotent producer, i.e. without a transactional id.
     *
     *   Size                 => 00 00 00 14 (20 bytes)
     *   ApiKey               => 00 16 (22)
     *   ApiVersion           => 00 00
     *   CorrelationId        => 00 00 00 07
     *   ClientId             => 00 04 "test"
     *   TransactionalId      => ff ff (null)
     *   TransactionTimeoutMs => 00 00 ea 60 (60000)
     */
    private const string REQUEST_HEX = '00000014'
        . '0016'
        . '0000'
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
        . '0000'
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

    public function testTheRequestOfAnIdempotentProducerCarriesANullTransactionalId(): void
    {
        $request = new InitProducerIdRequest(null, 60000, 'test', 7);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::INIT_PRODUCER_ID, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertNull($request->getTransactionalId());
        self::assertSame(60000, $request->getTransactionTimeoutMs());
    }

    public function testTheTransactionTimeoutIsTheJavaDefaultOfOneMinute(): void
    {
        self::assertSame(60000, InitProducerIdRequest::DEFAULT_TRANSACTION_TIMEOUT_MS);
        self::assertSame(
            InitProducerIdRequest::DEFAULT_TRANSACTION_TIMEOUT_MS,
            new InitProducerIdRequest()->getTransactionTimeoutMs()
        );
    }

    public function testTheRequestOfATransactionalProducerCarriesItsId(): void
    {
        $request = new InitProducerIdRequest('tx-42', 30000, 'test', 8);

        self::assertSame(self::TRANSACTIONAL_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame('tx-42', $request->getTransactionalId());
    }

    public function testTheRequestIsReadBackFieldByField(): void
    {
        $request = InitProducerIdRequest::unpack(new StringStream((string) hex2bin(self::TRANSACTIONAL_REQUEST_HEX)));

        self::assertSame('tx-42', $request->getTransactionalId());
        self::assertSame(30000, $request->getTransactionTimeoutMs());
        self::assertSame(self::TRANSACTIONAL_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testTheAnswerCarriesTheProducerIdAndTheEpoch(): void
    {
        $response = InitProducerIdResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(2000, $response->producerId);
        self::assertSame(3, $response->producerEpoch);
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testAnAnswerWithAnErrorCarriesNoProducerStateAtAll(): void
    {
        $response = InitProducerIdResponse::unpack(new StringStream((string) hex2bin(self::ERROR_RESPONSE_HEX)));

        self::assertSame(KafkaException::INVALID_TRANSACTION_TIMEOUT, $response->errorCode);
        self::assertSame(RecordBatch::NO_PRODUCER_ID, $response->producerId);
        self::assertSame(RecordBatch::NO_PRODUCER_EPOCH, $response->producerEpoch);
        self::assertSame(self::ERROR_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testTheEmptyStringIsATransactionalIdAndNotANullOne(): void
    {
        // The broker answers it with the error code 42, but only because it is a real, empty string on the wire
        $frame = bin2hex((string) new InitProducerIdRequest('', 60000, 'test', 7));

        self::assertStringContainsString('0000' . '0000ea60', $frame);
        self::assertStringNotContainsString('ffff' . '0000ea60', $frame);
    }
}
