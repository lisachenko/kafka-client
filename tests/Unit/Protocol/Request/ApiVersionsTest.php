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
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV0;

/**
 * Byte-exact tests for the ApiVersions API (api key 18, v0 and v1).
 *
 * Kafka 0.10.0 added version 0 and Kafka 0.11 version 1, which is the same request with a `throttle_time_ms`
 * appended to the answer - the one api of the release that puts the field at the end instead of the beginning.
 *
 * @see docs/protocol/0.11.0.md, section "ApiVersions API (key 18, v0 and v1)"
 */
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsRequestV0::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseV0::class)]
#[CoversClass(ApiVersionsResponseMetadata::class)]
final class ApiVersionsTest extends TestCase
{
    /**
     * ApiVersions request v1, which is the request header and nothing else.
     *
     *   Size          => 00 00 00 0e (14 bytes)
     *   ApiKey        => 00 12 (18)
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     */
    private const string REQUEST_HEX = '0000000e'
        . '0012'
        . '0001'
        . '00000001'
        . '0004' . '74657374';

    /**
     * The same request as version 0: only the ApiVersion field of the header differs.
     */
    private const string REQUEST_V0_HEX = '0000000e'
        . '0012'
        . '0000'
        . '00000001'
        . '0004' . '74657374';

    /**
     * ApiVersions response v0 with the three keys 0, 7 and 18, enough to show every shape of a version range.
     *
     *   Size          => 00 00 00 1c (28 bytes)
     *   CorrelationId => 00 00 00 01
     *   ErrorCode     => 00 00
     *   ApiVersions   => 00 00 00 03
     *     ApiKey/Min/Max => 00 00 00 00 00 03   (Produce, v0-v3)
     *     ApiKey/Min/Max => 00 07 00 01 00 01   (ControlledShutdown, v1 only)
     *     ApiKey/Min/Max => 00 12 00 00 00 01   (ApiVersions, v0-v1)
     */
    private const string RESPONSE_V0_HEX = '0000001c'
        . '00000001'
        . '0000'
        . '00000003'
        . '0000' . '0000' . '0003'
        . '0007' . '0001' . '0001'
        . '0012' . '0000' . '0001';

    /**
     * The same answer as version 1: the identical body plus the four bytes of the trailing throttle time.
     *
     *   Size            => 00 00 00 20 (32 bytes)
     *   …
     *   ThrottleTimeMs  => 00 00 00 00
     */
    private const string RESPONSE_HEX = '00000020'
        . '00000001'
        . '0000'
        . '00000003'
        . '0000' . '0000' . '0003'
        . '0007' . '0001' . '0001'
        . '0012' . '0000' . '0001'
        . '00000000';

    /**
     * The answer to an ApiVersions request of a version the broker does not serve: code 35 and no api at all.
     *
     * The broker writes it in the version 0 layout whichever version was asked for, so it has no throttle time.
     *
     *   Size          => 00 00 00 0a (10 bytes)
     *   CorrelationId => 00 00 00 65
     *   ErrorCode     => 00 23 (35, UnsupportedVersion)
     *   ApiVersions   => 00 00 00 00
     */
    private const string UNSUPPORTED_VERSION_RESPONSE_HEX = '0000000a'
        . '00000065'
        . '0023'
        . '00000000';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new ApiVersionsRequest('test', 1);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::API_VERSIONS, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'this client sends the version 1 of Kafka 0.11');
        self::assertSame(14, $request->getMessageSize(), 'header only, the request has no body');
    }

    /**
     * `API_VERSIONS_REQUEST_V1 = API_VERSIONS_REQUEST_V0`, so the two frames differ in one field of the header
     */
    public function testTheVersionZeroRequestIsTheSameFrameWithAnotherVersion(): void
    {
        $request = new ApiVersionsRequestV0('test', 1);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(14, $request->getMessageSize());
        self::assertSame(
            substr(self::REQUEST_HEX, 16),
            substr(self::REQUEST_V0_HEX, 16),
            'everything behind the api version is identical'
        );
    }

    public function testRequestWithoutAClientIdIsTwoBytesShorter(): void
    {
        $request = new ApiVersionsRequest();

        self::assertSame('0000000a' . '0012' . '0001' . '00000000' . '0000', bin2hex((string) $request));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(
            [ApiKeys::PRODUCE, ApiKeys::CONTROLLED_SHUTDOWN, ApiKeys::API_VERSIONS],
            array_keys($response->apiVersions),
            'the apis are indexed by their api key'
        );

        $produce = $response->apiVersions[ApiKeys::PRODUCE];
        self::assertSame(ApiKeys::PRODUCE, $produce->apiKey);
        self::assertSame(0, $produce->minVersion);
        self::assertSame(3, $produce->maxVersion);

        $controlledShutdown = $response->apiVersions[ApiKeys::CONTROLLED_SHUTDOWN];
        self::assertSame(1, $controlledShutdown->minVersion, 'version 0 is retired in Kafka 0.10');
        self::assertSame(1, $controlledShutdown->maxVersion);
    }

    /**
     * The throttle time closes the version 1 answer, so a version 0 frame is the same bytes without the last four
     */
    public function testTheThrottleTimeIsAppendedToTheAnswerAndNotPrepended(): void
    {
        $versionZero = ApiVersionsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_V0_HEX)));
        $versionOne  = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame($versionZero->getMessageSize() + 4, $versionOne->getMessageSize());
        self::assertSame($versionZero->errorCode, $versionOne->errorCode);
        self::assertSame(array_keys($versionZero->apiVersions), array_keys($versionOne->apiVersions));
        self::assertSame(
            substr(self::RESPONSE_HEX, 16, -8),
            substr(self::RESPONSE_V0_HEX, 16),
            'the body of the two versions is identical up to the trailing throttle time'
        );
        self::assertSame(0, $versionZero->throttleTimeMs, 'the field defaults to 0 when the frame does not carry it');
    }

    public function testResponseIsRepackedByteForByte(): void
    {
        $versionOne  = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $versionZero = ApiVersionsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_V0_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $versionOne));
        self::assertSame(self::RESPONSE_V0_HEX, bin2hex((string) $versionZero));
    }

    public function testSupportsAndMaxVersionAnswerFromTheRangeOfOneApi(): void
    {
        $response = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertTrue($response->supports(ApiKeys::PRODUCE, 0));
        self::assertTrue($response->supports(ApiKeys::PRODUCE, 3));
        self::assertFalse($response->supports(ApiKeys::PRODUCE, 4));
        self::assertFalse($response->supports(ApiKeys::CONTROLLED_SHUTDOWN, 0), 'the minimum version is inclusive');
        self::assertTrue($response->supports(ApiKeys::CONTROLLED_SHUTDOWN, 1));
        self::assertFalse($response->supports(ApiKeys::FETCH, 0), 'an api the broker does not report at all');

        self::assertSame(3, $response->maxVersionOf(ApiKeys::PRODUCE));
        self::assertSame(1, $response->maxVersionOf(ApiKeys::API_VERSIONS));
        self::assertNull($response->maxVersionOf(ApiKeys::FETCH));
    }

    public function testAnUnsupportedVersionIsReportedWithTheErrorCodeAndNoApis(): void
    {
        $response = ApiVersionsResponseV0::unpack(
            new StringStream((string) hex2bin(self::UNSUPPORTED_VERSION_RESPONSE_HEX))
        );

        self::assertSame(101, $response->getCorrelationId());
        self::assertSame(KafkaException::UNSUPPORTED_VERSION, $response->errorCode);
        self::assertSame([], $response->apiVersions);
        self::assertNull($response->maxVersionOf(ApiKeys::PRODUCE));
        self::assertSame(self::UNSUPPORTED_VERSION_RESPONSE_HEX, bin2hex((string) $response));
    }
}
