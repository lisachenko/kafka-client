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
use Protocol\Kafka\Common\Errors\UnsupportedSaslMechanismException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SaslToken;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\SaslHandshakeRequest;
use Protocol\Kafka\Protocol\Request\SaslHandshakeResponse;

/**
 * Byte-exact tests for the SaslHandshake API (key 17, v0, Kafka 0.10.0 / KIP-43) and for the token frames that
 * follow it.
 *
 * @see docs/protocol/0.10.2.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
 */
#[CoversClass(SaslHandshakeRequest::class)]
#[CoversClass(SaslHandshakeResponse::class)]
#[CoversClass(SaslToken::class)]
#[CoversClass(SaslMechanism::class)]
final class SaslHandshakeTest extends TestCase
{
    /**
     * SaslHandshake request v0 selecting the PLAIN mechanism, correlation id 1, client id "test".
     *
     *   Size          => 00 00 00 15 (21 bytes)
     *   ApiKey        => 00 11 (17)
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   Mechanism     => 00 05 "PLAIN"
     */
    private const string REQUEST_HEX = '00000015'
        . '0011'
        . '0000'
        . '00000001'
        . '0004' . '74657374'
        . '0005' . '504c41494e';

    /**
     * The answer of a broker that accepted PLAIN and has exactly that mechanism enabled.
     *
     *   Size              => 00 00 00 11 (17 bytes)
     *   CorrelationId     => 00 00 00 01
     *   ErrorCode         => 00 00
     *   EnabledMechanisms => 00 00 00 01, 00 05 "PLAIN"
     */
    private const string RESPONSE_HEX = '00000011'
        . '00000001'
        . '0000'
        . '00000001' . '0005' . '504c41494e';

    /**
     * The answer to a mechanism the broker has not enabled: error code 33 and the list of the enabled ones.
     */
    private const string UNSUPPORTED_RESPONSE_HEX = '00000011'
        . '00000001'
        . '0021'
        . '00000001' . '0005' . '504c41494e';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new SaslHandshakeRequest(SaslMechanism::PLAIN, 'test', 1);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::SASL_HANDSHAKE, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'a 0.10.2.2 broker serves version 0 only');
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = SaslHandshakeResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame([SaslMechanism::PLAIN], $response->enabledMechanisms);
    }

    public function testUnsupportedMechanismIsReportedWithTheEnabledOnes(): void
    {
        $response = SaslHandshakeResponse::unpack(new StringStream((string) hex2bin(self::UNSUPPORTED_RESPONSE_HEX)));

        self::assertSame(KafkaException::UNSUPPORTED_SASL_MECHANISM, $response->errorCode);
        self::assertSame([SaslMechanism::PLAIN], $response->enabledMechanisms);
        self::assertInstanceOf(
            UnsupportedSaslMechanismException::class,
            KafkaException::fromCode($response->errorCode),
            'the error code 33 of Kafka 0.10.0 maps onto its own exception'
        );
    }

    /**
     * The token of the PLAIN mechanism is `authzid \0 authcid \0 passwd` (RFC 4616) in a bare size-prefixed frame:
     * no api key, no version, no correlation id - the request that wraps it is Kafka 1.0.
     */
    public function testPlainTokenIsPackedAsASizePrefixedFrame(): void
    {
        $token = SaslToken::ofPlainCredentials('kafkatest', 'kafkatest-secret');

        self::assertSame("\0kafkatest\0kafkatest-secret", $token->token);
        self::assertSame(
            '0000001b' . '00' . '6b61666b6174657374' . '00' . '6b61666b61746573742d736563726574',
            bin2hex($token->pack())
        );
        self::assertFalse($token->isEmpty());
    }

    public function testAuthorizationIdIsSentWhenItIsGiven(): void
    {
        $token = SaslToken::ofPlainCredentials('admin', 'admin-secret', 'kafkatest');

        self::assertSame("kafkatest\0admin\0admin-secret", $token->token);
    }

    public function testEmptyTokenIsTheAnswerOfASuccessfulPlainExchange(): void
    {
        $answer = SaslToken::unpack((string) hex2bin('00000000'));

        self::assertSame('', $answer->token);
        self::assertTrue($answer->isEmpty());
        self::assertSame('00000000', bin2hex($answer->pack()));
    }

    public function testNullTokenIsReadAsAnEmptyOne(): void
    {
        $answer = SaslToken::unpack((string) hex2bin('ffffffff'));

        self::assertNull($answer->token, 'the bytes primitive defines -1 as null');
        self::assertTrue($answer->isEmpty());
    }

    public function testOnlyPlainIsImplementedOfTheMechanismsOfTheBroker(): void
    {
        self::assertSame([SaslMechanism::PLAIN], SaslMechanism::implemented());
        self::assertSame(
            ['GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'],
            SaslMechanism::all(),
            'a Kafka 0.10.2.2 broker knows GSSAPI (0.9), PLAIN (0.10.0) and the SCRAM mechanisms (0.10.2)'
        );
    }
}
