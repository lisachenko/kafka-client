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
use Protocol\Kafka\Common\Errors\IllegalSaslStateException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\SaslAuthenticationFailedException;
use Protocol\Kafka\Common\Security\SaslToken;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateRequest;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateRequestV0;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateRequestV1;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateResponse;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateResponseV0;
use Protocol\Kafka\Protocol\Request\SaslAuthenticateResponseV1;

/**
 * Byte-exact tests for the SaslAuthenticate API (key 36, v0, Kafka 1.0 / KIP-152).
 *
 * The request carries the token that a v0 exchange would write on the socket raw, and the answer is the one thing
 * the raw exchange never had: an error code with a message.
 *
 * @see docs/protocol/2.8.md, section "SaslAuthenticate API (key 36, v0 to v2)"
 */
#[CoversClass(SaslAuthenticateRequest::class)]
#[CoversClass(SaslAuthenticateRequestV0::class)]
#[CoversClass(SaslAuthenticateRequestV1::class)]
#[CoversClass(SaslAuthenticateResponse::class)]
#[CoversClass(SaslAuthenticateResponseV0::class)]
#[CoversClass(SaslAuthenticateResponseV1::class)]
#[CoversClass(SaslToken::class)]
final class SaslAuthenticateTest extends TestCase
{
    /**
     * SaslAuthenticate request v2 carrying the PLAIN token of `kafkatest`, correlation id 2, client id "test".
     *
     * The first flexible version of the api (Kafka 2.5, KIP-482): the request header v2 with its tagged-field
     * section, the token as a compact `bytes` field and the tag buffer that closes the body.
     *
     *   Size          => 00 00 00 2c (44 bytes)
     *   ApiKey        => 00 24 (36)
     *   ApiVersion    => 00 02
     *   CorrelationId => 00 00 00 02
     *   ClientId      => 00 04 "test"   (an int16 string even in a flexible frame)
     *   TAG_BUFFER    => 00
     *   SaslAuthBytes => 1c (27 + 1), "\0kafkatest\0kafkatest-secret"
     *   TAG_BUFFER    => 00
     */
    private const string REQUEST_HEX = '0000002c'
        . '0024'
        . '0002'
        . '00000002'
        . '0004' . '74657374'
        . '00'
        . '1c' . '006b61666b6174657374006b61666b61746573742d736563726574'
        . '00';

    /**
     * The same token in the non-flexible frame of the versions 0 and 1.
     *
     *   Size          => 00 00 00 2d (45 bytes)
     *   ApiVersion    => 00 01
     *   SaslAuthBytes => 00 00 00 1b, "\0kafkatest\0kafkatest-secret"
     */
    private const string REQUEST_V1_HEX = '0000002d'
        . '0024'
        . '0001'
        . '00000002'
        . '0004' . '74657374'
        . '0000001b' . '006b61666b6174657374006b61666b61746573742d736563726574';

    /**
     * The answer of a completed PLAIN exchange: no error, no message, and the empty token of the mechanism.
     *
     *   Size            => 00 00 00 0c (12 bytes)
     *   CorrelationId   => 00 00 00 02
     *   ErrorCode       => 00 00
     *   ErrorMessage    => ff ff (null)
     *   SaslAuthBytes   => 00 00 00 00
     */
    private const string RESPONSE_HEX = '0000000c' . '00000002' . '0000' . 'ffff' . '00000000';

    /**
     * The answer to a wrong password: the error code 58 and the message of the broker.
     */
    private const string REFUSED_RESPONSE_HEX = '0000003f'
        . '00000002'
        . '003a'
        . '0033' . '41757468656e7469636174696f6e206661696c65643a20496e76616c696420757365726e616d65206f722070617373776f7264'
        . '00000000';

    /**
     * The answer to a request that arrived after the authentication was already complete: the error code 34.
     */
    private const string ILLEGAL_STATE_RESPONSE_HEX = '0000004d'
        . '00000002'
        . '0022'
        . '0041' . '5361736c41757468656e74696361746520726571756573742072656365697665642061667465'
        . '72207375636365737366756c2061757468656e7469636174696f6e'
        . '00000000';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $token   = SaslToken::ofPlainCredentials('kafkatest', 'kafkatest-secret');
        $request = new SaslAuthenticateRequest($token->token, 'test', 2);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::SASL_AUTHENTICATE, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion(), 'Kafka 2.5 raised the api to the flexible version 2');
        self::assertTrue(SaslAuthenticateRequest::isFlexible(), 'and the version 2 is the first flexible one');
        self::assertSame(
            self::REQUEST_V1_HEX,
            bin2hex((string) new SaslAuthenticateRequestV1($token->token, 'test', 2)),
            'the version 1 keeps the frame of KIP-368'
        );
        self::assertSame(
            substr_replace(self::REQUEST_V1_HEX, '0000', 12, 4),
            bin2hex((string) new SaslAuthenticateRequestV0($token->token, 'test', 2)),
            'SASL_AUTHENTICATE_REQUEST_V1 = SASL_AUTHENTICATE_REQUEST_V0 @ 2.2.2: only the version field differs'
        );
    }

    /**
     * The token of the request is byte for byte the payload of the frame a v0 exchange writes on the socket: the
     * length prefix of the `bytes` field is the same `INT32` the bare token frame carries.
     */
    public function testTheRequestCarriesExactlyTheBytesOfTheRawToken(): void
    {
        $token = SaslToken::ofPlainCredentials('kafkatest', 'kafkatest-secret');
        $frame = bin2hex((string) new SaslAuthenticateRequestV1($token->token, 'test', 2));

        self::assertStringEndsWith(bin2hex($token->pack()), $frame);

        // The flexible version carries the very same bytes behind a compact length instead of the int32 one
        self::assertStringEndsWith(
            '1c' . bin2hex($token->token) . '00',
            bin2hex((string) new SaslAuthenticateRequest($token->token, 'test', 2))
        );
    }

    /**
     * The answer of version 1 is the answer of version 0 plus the `session_lifetime_ms` of KIP-368
     */
    public function testTheAnswerOfVersionOneCarriesTheSessionLifetimeOfKip368(): void
    {
        // Captured on the container: the empty token of a finished PLAIN exchange, the empty error message a
        // 2.8.2 broker sends instead of a null one, and the lifetime 0 of a listener without
        // `connections.max.reauth.ms`
        $hex      = '00000014' . '00000322' . '0000' . '0000' . '00000000' . '0000000000000000';
        $response = SaslAuthenticateResponseV1::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(802, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame('', $response->errorMessage);
        self::assertSame('', $response->saslAuthBytes);
        self::assertSame(0, $response->sessionLifetimeMs, 'zero means that the session never expires');
        self::assertSame($hex, bin2hex((string) $response));
        self::assertSame(
            ['messageSize', 'correlationId', 'errorCode', 'errorMessage', 'saslAuthBytes'],
            array_keys(SaslAuthenticateResponseV0::getScheme()),
            'and the answer of version 0 has no such field'
        );
    }

    /**
     * The answer of version 2 is the answer of version 1 in the compact encoding, with two tagged-field sections
     */
    public function testTheAnswerOfVersionTwoIsTheFlexibleFrameOfKip482(): void
    {
        // Captured on the container over the SASL_PLAINTEXT listener: the response header v1 with its tag buffer,
        // the empty compact error message, the empty compact token, the lifetime 0 and the tag buffer of the body
        $hex      = '00000012' . '00000394' . '00' . '0000' . '01' . '01' . '0000000000000000' . '00';
        $response = SaslAuthenticateResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(916, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame('', $response->errorMessage);
        self::assertSame('', $response->saslAuthBytes);
        self::assertSame(0, $response->sessionLifetimeMs);
        self::assertSame($hex, bin2hex((string) $response));
        self::assertTrue(SaslAuthenticateResponse::isFlexible());
    }

    public function testResponseOfACompletedExchangeIsUnpackedAccordingToTheSpec(): void
    {
        $response = SaslAuthenticateResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(2, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertNull($response->errorMessage, 'a successful answer carries no message');
        self::assertSame('', $response->saslAuthBytes, 'PLAIN answers with the empty token');
    }

    public function testRefusedCredentialsCarryTheErrorCode58AndTheMessageOfTheBroker(): void
    {
        $response = SaslAuthenticateResponseV0::unpack(new StringStream((string) hex2bin(self::REFUSED_RESPONSE_HEX)));

        self::assertSame(KafkaException::SASL_AUTHENTICATION_FAILED, $response->errorCode);
        self::assertSame('Authentication failed: Invalid username or password', $response->errorMessage);
        self::assertSame('', $response->saslAuthBytes, 'an answer with an error code carries no token');
        self::assertInstanceOf(
            SaslAuthenticationFailedException::class,
            KafkaException::fromCode($response->errorCode),
            'the error code 58 of Kafka 1.0 maps onto its own exception'
        );
    }

    public function testARequestAfterTheAuthenticationIsAnsweredWithTheIllegalSaslState(): void
    {
        $response = SaslAuthenticateResponseV0::unpack(
            new StringStream((string) hex2bin(self::ILLEGAL_STATE_RESPONSE_HEX))
        );

        self::assertSame(KafkaException::ILLEGAL_SASL_STATE, $response->errorCode);
        self::assertSame('SaslAuthenticate request received after successful authentication', $response->errorMessage);
        self::assertInstanceOf(IllegalSaslStateException::class, KafkaException::fromCode($response->errorCode));
    }

    public function testEveryAnswerSurvivesADecodeAndEncodeRoundTrip(): void
    {
        foreach ([self::RESPONSE_HEX, self::REFUSED_RESPONSE_HEX, self::ILLEGAL_STATE_RESPONSE_HEX] as $hex) {
            $response = SaslAuthenticateResponseV0::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($hex, bin2hex((string) $response));
        }
    }
}
