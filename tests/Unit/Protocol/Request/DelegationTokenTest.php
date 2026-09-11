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
use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeDelegationTokenResponseToken;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenRequestV0;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenRequestV1;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenResponseV0;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenResponseV1;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenResponseV0;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenRequestV0;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenResponseV0;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenRequestV0;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenResponseV0;

/**
 * Byte-exact tests for the four delegation token APIs of KIP-48 (api keys 38 to 41), at the version 1 that
 * Kafka 2.0 added to all four of them.
 *
 * The frames of the four apis share two properties that no api of the lines below has, and both are pinned here:
 * `throttle_time_ms` is the **last** field of every answer instead of the first one, and every principal on the
 * wire - the owner of a token, the renewers of a request, the owners of a describe request - is the two-string
 * struct {@see KafkaPrincipal}.
 *
 * @see docs/protocol/2.8.md, sections "Delegation tokens (KIP-48)", "CreateDelegationToken API (key 38, v0 to v2)",
 *      "RenewDelegationToken API (key 39, v0 and v1)", "ExpireDelegationToken API (key 40, v0 and v1)" and
 *      "DescribeDelegationToken API (key 41, v0 and v1)"
 */
#[CoversClass(CreateDelegationTokenRequest::class)]
#[CoversClass(CreateDelegationTokenRequestV0::class)]
#[CoversClass(CreateDelegationTokenResponse::class)]
#[CoversClass(CreateDelegationTokenResponseV0::class)]
#[CoversClass(RenewDelegationTokenRequest::class)]
#[CoversClass(RenewDelegationTokenRequestV0::class)]
#[CoversClass(RenewDelegationTokenResponse::class)]
#[CoversClass(RenewDelegationTokenResponseV0::class)]
#[CoversClass(ExpireDelegationTokenRequest::class)]
#[CoversClass(ExpireDelegationTokenRequestV0::class)]
#[CoversClass(ExpireDelegationTokenResponse::class)]
#[CoversClass(ExpireDelegationTokenResponseV0::class)]
#[CoversClass(DescribeDelegationTokenRequest::class)]
#[CoversClass(DescribeDelegationTokenRequestV0::class)]
#[CoversClass(DescribeDelegationTokenResponse::class)]
#[CoversClass(DescribeDelegationTokenResponseV0::class)]
#[CoversClass(DescribeDelegationTokenResponseToken::class)]
final class DelegationTokenTest extends TestCase
{
    /**
     * CreateDelegationToken request v1 with one renewer and a maximum lifetime of one hour.
     *
     *   Size          => 00 00 00 27 (39 bytes)
     *   ApiKey        => 00 26 (38)
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 05
     *   ClientId      => 00 04 "test"
     *   Renewers      => 00 00 00 01
     *     PrincipalType => 00 04 "User", Name => 00 05 "admin"
     *   MaxLifeTime   => 00 00 00 00 00 36 ee 80 (3600000)
     */
    private const string CREATE_REQUEST_HEX = '00000027'
        . '0026'
        . '0001'
        . '00000005'
        . '0004' . '74657374'
        . '00000001'
        . '0004' . '55736572'
        . '0005' . '61646d696e'
        . '000000000036ee80';

    /**
     * The same request as the **flexible** version 2 of Kafka 2.4, which is what this client sends.
     *
     *   Size          => 00 00 00 25 (37 bytes, two less than v1)
     *   ApiKey        => 00 26, ApiVersion => 00 02
     *   CorrelationId => 00 00 00 05
     *   ClientId      => 00 04 "test"   (int16 length even here)
     *   TAG_BUFFER    => 00             (of the request header v2)
     *   Renewers      => 02             (compact: one renewer)
     *     PrincipalType => 05 "User", Name => 06 "admin", TAG_BUFFER => 00
     *   MaxLifeTime   => 00 00 00 00 00 36 ee 80
     *   TAG_BUFFER    => 00             (of the body)
     */
    private const string CREATE_REQUEST_V2_HEX = '00000025'
        . '0026'
        . '0002'
        . '00000005'
        . '0004' . '74657374'
        . '00'
        . '02'
        . '05' . '55736572'
        . '06' . '61646d696e'
        . '00'
        . '000000000036ee80'
        . '00';

    /**
     * The version 2 answer of the same token: compact strings, a compact hmac and two tag buffers, and the owner
     * inlined without one, because `PrincipalType` and `PrincipalName` are two fields of the answer and not a
     * structure of the specification.
     */
    private const string CREATE_RESPONSE_V2_HEX = '00000041'
        . '00000005'
        . '00'
        . '0000'
        . '05' . '55736572'
        . '0a' . '6b61666b6174657374'
        . '00000174876e8000'
        . '0000017487a56e80'
        . '0000017487a56e80'
        . '09' . '746f6b656e2d6964'
        . '05' . 'deadbeef'
        . '00000000'
        . '00';

    /**
     * The same request without a renewer and with the default maximum lifetime, i.e. the smallest frame of the api
     */
    private const string CREATE_REQUEST_DEFAULT_HEX = '0000001a'
        . '0026'
        . '0001'
        . '00000005'
        . '0004' . '74657374'
        . '00000000'
        . 'ffffffffffffffff';

    /**
     * CreateDelegationToken answer v0 for the token `token-id` of `User:kafkatest`.
     *
     *   Size            => 00 00 00 45 (69 bytes)
     *   CorrelationId   => 00 00 00 05
     *   ErrorCode       => 00 00
     *   Owner           => 00 04 "User", 00 09 "kafkatest"
     *   IssueTimestamp  => 1600000000000
     *   ExpiryTimestamp => 1600003600000
     *   MaxTimestamp    => 1600003600000
     *   TokenId         => 00 08 "token-id"
     *   Hmac            => 00 00 00 04 de ad be ef
     *   ThrottleTimeMs  => 00 00 00 00
     */
    private const string CREATE_RESPONSE_HEX = '00000045'
        . '00000005'
        . '0000'
        . '0004' . '55736572'
        . '0009' . '6b61666b6174657374'
        . '00000174876e8000'
        . '0000017487a56e80'
        . '0000017487a56e80'
        . '0008' . '746f6b656e2d6964'
        . '00000004' . 'deadbeef'
        . '00000000';

    /**
     * The same answer for a renewer whose principal type is not `User`: the error code 67 and no token at all
     */
    private const string CREATE_RESPONSE_INVALID_PRINCIPAL_HEX = '00000039'
        . '00000005'
        . '0043'
        . '0004' . '55736572'
        . '0009' . '6b61666b6174657374'
        . 'ffffffffffffffff'
        . 'ffffffffffffffff'
        . 'ffffffffffffffff'
        . '0000'
        . '00000000'
        . '00000000';

    /**
     * RenewDelegationToken request v0 that asks for ten more minutes of life for the hmac `01 02 03 04`
     */
    private const string RENEW_REQUEST_HEX = '0000001e'
        . '0027'
        . '0001'
        . '00000006'
        . '0004' . '74657374'
        . '00000004' . '01020304'
        . '00000000000927c0';

    /**
     * RenewDelegationToken answer v0 with the new expiry, and the throttle time behind it
     */
    private const string RENEW_RESPONSE_HEX = '00000012'
        . '00000006'
        . '0000'
        . '0000017487a56e80'
        . '00000000';

    /**
     * The same answer for a principal that may not renew the token: the code 63 and the timestamp -1
     */
    private const string RENEW_RESPONSE_MISMATCH_HEX = '00000012'
        . '00000006'
        . '003f'
        . 'ffffffffffffffff'
        . '00000000';

    /**
     * ExpireDelegationToken request v0 with a negative period, i.e. "delete the token now"
     */
    private const string EXPIRE_REQUEST_HEX = '0000001e'
        . '0028'
        . '0001'
        . '00000007'
        . '0004' . '74657374'
        . '00000004' . '01020304'
        . 'ffffffffffffffff';

    /**
     * ExpireDelegationToken answer v0 of a token that was deleted: the clock of the broker
     */
    private const string EXPIRE_RESPONSE_HEX = '00000012'
        . '00000007'
        . '0000'
        . '00000174876e8000'
        . '00000000';

    /**
     * DescribeDelegationToken request v0 with a **null** owner array, i.e. "every token I may see"
     */
    private const string DESCRIBE_REQUEST_ALL_HEX = '00000012'
        . '0029'
        . '0001'
        . '00000008'
        . '0004' . '74657374'
        . 'ffffffff';

    /**
     * The same request with an **empty** owner array, which asks for nothing at all
     */
    private const string DESCRIBE_REQUEST_NONE_HEX = '00000012'
        . '0029'
        . '0001'
        . '00000008'
        . '0004' . '74657374'
        . '00000000';

    /**
     * The same request for one named owner
     */
    private const string DESCRIBE_REQUEST_ONE_HEX = '00000023'
        . '0029'
        . '0001'
        . '00000008'
        . '0004' . '74657374'
        . '00000001'
        . '0004' . '55736572'
        . '0009' . '6b61666b6174657374';

    /**
     * DescribeDelegationToken answer v0 with one token that names one renewer
     */
    private const string DESCRIBE_RESPONSE_HEX = '0000005a'
        . '00000008'
        . '0000'
        . '00000001'
        . '0004' . '55736572'
        . '0009' . '6b61666b6174657374'
        . '00000174876e8000'
        . '0000017487a56e80'
        . '0000017487a56e80'
        . '0008' . '746f6b656e2d6964'
        . '00000004' . 'deadbeef'
        . '00000001'
        . '0004' . '55736572'
        . '0005' . '61646d696e'
        . '00000000';

    public function testTheCreateRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new CreateDelegationTokenRequestV1([KafkaPrincipal::user('admin')], 3600000, 'test', 5);

        self::assertSame(self::CREATE_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::CREATE_DELEGATION_TOKEN, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised all four apis to version 1 (KIP-219)');
        self::assertSame(39, $request->getMessageSize());
    }

    /**
     * Version 2 is the same request in the flexible encoding of Kafka 2.4, and it is the one the client sends
     */
    public function testTheCreateRequestOfVersionTwoIsCompact(): void
    {
        $request = new CreateDelegationTokenRequest([KafkaPrincipal::user('admin')], 3600000, 'test', 5);

        self::assertSame(self::CREATE_REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion(), 'Kafka 2.4 raised the api to the flexible version 2');
        self::assertTrue(CreateDelegationTokenRequest::isFlexible());
        self::assertSame(37, $request->getMessageSize(), 'two bytes shorter than v1: four length prefixes, two tag buffers');
    }

    public function testARenewerCanBeGivenAsThePrincipalStringOfTheKafkaTools(): void
    {
        $fromString = new CreateDelegationTokenRequestV1(['User:admin'], 3600000, 'test', 5);

        self::assertSame(self::CREATE_REQUEST_HEX, bin2hex((string) $fromString));
        self::assertEquals([KafkaPrincipal::user('admin')], $fromString->getRenewers());
    }

    public function testACreateRequestWithoutRenewersAsksForTheDefaultLifetimeOfTheBroker(): void
    {
        $request = new CreateDelegationTokenRequestV1();

        self::assertSame(-1, CreateDelegationTokenRequest::DEFAULT_MAX_LIFE_TIME);
        self::assertSame([], $request->getRenewers());
        self::assertSame(-1, $request->getMaxLifeTime());
        self::assertSame(
            self::CREATE_REQUEST_DEFAULT_HEX,
            bin2hex((string) new CreateDelegationTokenRequestV1([], -1, 'test', 5))
        );
    }

    public function testTheCreateResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = CreateDelegationTokenResponseV1::unpack(
            new StringStream((string) hex2bin(self::CREATE_RESPONSE_HEX))
        );

        self::assertSame(5, $response->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame('User', $response->owner->principalType);
        self::assertSame('kafkatest', $response->owner->name);
        self::assertSame(1600000000000, $response->issueTimestamp);
        self::assertSame(1600003600000, $response->expiryTimestamp);
        self::assertSame(1600003600000, $response->maxTimestamp);
        self::assertSame('token-id', $response->tokenId);
        self::assertSame("\xde\xad\xbe\xef", $response->hmac);
        self::assertSame(0, $response->throttleTimeMs, 'the throttle time is the LAST field of the answer');
    }

    public function testAFailedCreateAnswerCarriesTheConnectionPrincipalAndNoToken(): void
    {
        $response = CreateDelegationTokenResponseV1::unpack(
            new StringStream((string) hex2bin(self::CREATE_RESPONSE_INVALID_PRINCIPAL_HEX))
        );

        self::assertSame(KafkaException::INVALID_PRINCIPAL_TYPE, $response->errorCode);
        self::assertSame('User:kafkatest', (string) $response->owner);
        self::assertSame(CreateDelegationTokenResponse::ERROR_TIMESTAMP, $response->issueTimestamp);
        self::assertSame(CreateDelegationTokenResponse::ERROR_TIMESTAMP, $response->expiryTimestamp);
        self::assertSame(CreateDelegationTokenResponse::ERROR_TIMESTAMP, $response->maxTimestamp);
        self::assertSame('', $response->tokenId);
        self::assertSame('', $response->hmac, 'an empty byte array, not a null one');
    }

    public function testTheRenewRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new RenewDelegationTokenRequest("\x01\x02\x03\x04", 600000, 'test', 6);

        self::assertSame(self::RENEW_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::RENEW_DELEGATION_TOKEN, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
        self::assertSame("\x01\x02\x03\x04", $request->getHmac());
        self::assertSame(600000, $request->getRenewTimePeriod());
        self::assertSame(-1, RenewDelegationTokenRequest::DEFAULT_RENEW_TIME_PERIOD);
    }

    public function testTheRenewResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = RenewDelegationTokenResponse::unpack(
            new StringStream((string) hex2bin(self::RENEW_RESPONSE_HEX))
        );

        self::assertSame(6, $response->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(1600003600000, $response->expiryTimestamp);
        self::assertSame(0, $response->throttleTimeMs);
    }

    public function testAnErrorOfTheRenewApiCarriesTheTimestampMinusOne(): void
    {
        $response = RenewDelegationTokenResponse::unpack(
            new StringStream((string) hex2bin(self::RENEW_RESPONSE_MISMATCH_HEX))
        );

        self::assertSame(KafkaException::DELEGATION_TOKEN_OWNER_MISMATCH, $response->errorCode);
        self::assertSame(CreateDelegationTokenResponse::ERROR_TIMESTAMP, $response->expiryTimestamp);
    }

    public function testTheExpireRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new ExpireDelegationTokenRequest("\x01\x02\x03\x04", -1, 'test', 7);

        self::assertSame(self::EXPIRE_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::EXPIRE_DELEGATION_TOKEN, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
        self::assertSame("\x01\x02\x03\x04", $request->getHmac());
        self::assertSame(-1, $request->getExpiryTimePeriod());
        self::assertSame(-1, ExpireDelegationTokenRequest::EXPIRE_IMMEDIATELY);
    }

    public function testTheExpireResponseHasTheSameFrameAsTheRenewOne(): void
    {
        $response = ExpireDelegationTokenResponse::unpack(
            new StringStream((string) hex2bin(self::EXPIRE_RESPONSE_HEX))
        );

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(1600000000000, $response->expiryTimestamp, 'the clock of the broker for a deleted token');
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(
            array_keys(RenewDelegationTokenResponse::getScheme()),
            array_keys(ExpireDelegationTokenResponse::getScheme()),
            'the two answers of KIP-48 have identical schemas'
        );
    }

    public function testTheDescribeRequestSendsANullArrayForEveryVisibleToken(): void
    {
        $request = new DescribeDelegationTokenRequest(null, 'test', 8);

        self::assertSame(self::DESCRIBE_REQUEST_ALL_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_DELEGATION_TOKEN, $request->getApiKey());
        self::assertNull($request->getOwners());
        self::assertNull(DescribeDelegationTokenRequest::ALL_OWNERS);
    }

    public function testAnEmptyOwnerArrayIsNotTheSameFrameAsANullOne(): void
    {
        self::assertSame(
            self::DESCRIBE_REQUEST_NONE_HEX,
            bin2hex((string) new DescribeDelegationTokenRequest([], 'test', 8)),
            'an empty array asks for nothing, a null array for everything'
        );
        self::assertNotSame(self::DESCRIBE_REQUEST_NONE_HEX, self::DESCRIBE_REQUEST_ALL_HEX);
    }

    public function testTheDescribeRequestPacksTheOwnersItIsGiven(): void
    {
        $request = new DescribeDelegationTokenRequest(['User:kafkatest'], 'test', 8);

        self::assertSame(self::DESCRIBE_REQUEST_ONE_HEX, bin2hex((string) $request));
        self::assertEquals([KafkaPrincipal::user('kafkatest')], $request->getOwners());
    }

    public function testTheDescribeResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DescribeDelegationTokenResponse::unpack(
            new StringStream((string) hex2bin(self::DESCRIBE_RESPONSE_HEX))
        );

        self::assertSame(8, $response->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(['token-id'], array_keys($response->tokenDetails), 'the tokens are indexed by their id');

        $token = $response->tokenDetails['token-id'];
        self::assertSame('User:kafkatest', (string) $token->owner);
        self::assertSame(1600000000000, $token->issueTimestamp);
        self::assertSame(1600003600000, $token->expiryTimestamp);
        self::assertSame(1600003600000, $token->maxTimestamp);
        self::assertSame('token-id', $token->tokenId);
        self::assertSame("\xde\xad\xbe\xef", $token->hmac, 'a described token carries its secret as well');
        self::assertSame(['User:admin'], array_map(strval(...), $token->renewers));
        self::assertSame(0, $response->throttleTimeMs);
    }

    public function testEveryAnswerOfTheGroupEndsWithItsThrottleTime(): void
    {
        $answers = [
            CreateDelegationTokenResponse::class,
            RenewDelegationTokenResponse::class,
            ExpireDelegationTokenResponse::class,
            DescribeDelegationTokenResponse::class,
        ];

        foreach ($answers as $answer) {
            $fields = array_keys($answer::getScheme());
            self::assertSame('throttleTimeMs', end($fields), "{$answer} does not end with the throttle time");
            // The response header is two fields below Kafka 2.4 and three from the flexible versions on, where it
            // ends in a tag buffer of its own, so the body is whatever follows it
            $body = array_values(array_diff($fields, ['messageSize', 'correlationId', 'headerTaggedFields']));
            self::assertSame(
                'errorCode',
                $body[0],
                "{$answer} does not open with the error code behind the response header"
            );
        }
    }

    /**
     * The four version 0 frames of Kafka 1.1, which Kafka 2.0 raised to version 1 without changing a byte
     * (`TOKEN_CREATE_REQUEST_V1 = TOKEN_CREATE_REQUEST_V0` and its three siblings in `Protocol.java` @ 2.0.1)
     */
    public function testTheVersionZeroFramesAreTheSameBodiesWithALowerVersionField(): void
    {
        $create   = new CreateDelegationTokenRequestV0([KafkaPrincipal::user('admin')], 3600000, 'test', 5);
        $renew    = new RenewDelegationTokenRequestV0("\x01\x02\x03\x04", 600000, 'test', 6);
        $expire   = new ExpireDelegationTokenRequestV0("\x01\x02\x03\x04", -1, 'test', 7);
        $describe = new DescribeDelegationTokenRequestV0(['User:kafkatest'], 'test', 8);

        self::assertSame(substr_replace(self::CREATE_REQUEST_HEX, '0000', 12, 4), bin2hex((string) $create));
        self::assertSame(substr_replace(self::RENEW_REQUEST_HEX, '0000', 12, 4), bin2hex((string) $renew));
        self::assertSame(substr_replace(self::EXPIRE_REQUEST_HEX, '0000', 12, 4), bin2hex((string) $expire));
        self::assertSame(substr_replace(self::DESCRIBE_REQUEST_ONE_HEX, '0000', 12, 4), bin2hex((string) $describe));
        self::assertSame([0, 0, 0, 0], [
            $create->getApiVersion(),
            $renew->getApiVersion(),
            $expire->getApiVersion(),
            $describe->getApiVersion(),
        ]);
    }

    /**
     * The four answers of version 0 have the layout of version 1, throttle time last included
     */
    public function testTheVersionZeroAnswersAreReadByTheClassesOfTheirOwnVersion(): void
    {
        $answers = [
            CreateDelegationTokenResponseV0::class   => self::CREATE_RESPONSE_HEX,
            RenewDelegationTokenResponseV0::class    => self::RENEW_RESPONSE_HEX,
            ExpireDelegationTokenResponseV0::class   => self::EXPIRE_RESPONSE_HEX,
            DescribeDelegationTokenResponseV0::class => self::DESCRIBE_RESPONSE_HEX,
        ];

        foreach ($answers as $class => $hex) {
            $response = $class::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($hex, bin2hex((string) $response), "{$class} does not survive a round trip");
            $fields = array_keys($class::getScheme());
            self::assertSame('throttleTimeMs', end($fields), "{$class} does not end with the throttle time");
        }
    }

}
