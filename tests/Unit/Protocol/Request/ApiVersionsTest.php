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
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV1;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV1;

/**
 * Byte-exact tests for the ApiVersions API (api key 18, v0 to v2).
 *
 * Kafka 0.10.0 added version 0, Kafka 0.11 version 1 - the same request with a `throttle_time_ms` appended to the
 * answer, the one api of that release that puts the field at the end instead of the beginning - and Kafka 2.0 the
 * version 2 of KIP-219, whose frame is the frame of version 1 and whose meaning is that the client waits out a
 * throttle time itself. The version 3 of Kafka 2.4 is the first flexible one and is not implemented here yet.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v2)"
 */
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsRequestV0::class)]
#[CoversClass(ApiVersionsRequestV1::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseV0::class)]
#[CoversClass(ApiVersionsResponseV1::class)]
#[CoversClass(ApiVersionsResponseMetadata::class)]
final class ApiVersionsTest extends TestCase
{
    /**
     * ApiVersions request v2, the version this client sends: the request header and nothing else.
     *
     *   Size          => 00 00 00 0e (14 bytes)
     *   ApiKey        => 00 12 (18)
     *   ApiVersion    => 00 02
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     */
    private const string REQUEST_HEX = '0000000e'
        . '0012'
        . '0002'
        . '00000001'
        . '0004' . '74657374';

    /**
     * The same request as version 1: only the ApiVersion field of the header differs.
     */
    private const string REQUEST_V1_HEX = '0000000e'
        . '0012'
        . '0001'
        . '00000001'
        . '0004' . '74657374';

    /**
     * The same request as version 0.
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
     *     ApiKey/Min/Max => 00 00 00 00 00 09   (Produce, v0-v9)
     *     ApiKey/Min/Max => 00 07 00 00 00 03   (ControlledShutdown, v0-v3)
     *     ApiKey/Min/Max => 00 12 00 00 00 03   (ApiVersions, v0-v3)
     */
    private const string RESPONSE_V0_HEX = '0000001c'
        . '00000001'
        . '0000'
        . '00000003'
        . '0000' . '0000' . '0009'
        . '0007' . '0000' . '0003'
        . '0012' . '0000' . '0003';

    /**
     * The same answer as version 2: the identical body plus the four bytes of the trailing throttle time.
     *
     *   Size            => 00 00 00 20 (32 bytes)
     *   …
     *   ThrottleTimeMs  => 00 00 00 00
     */
    private const string RESPONSE_HEX = '00000020'
        . '00000001'
        . '0000'
        . '00000003'
        . '0000' . '0000' . '0009'
        . '0007' . '0000' . '0003'
        . '0012' . '0000' . '0003'
        . '00000000';

    /**
     * The answer to an ApiVersions request of a version the broker does not serve, as Kafka 2.4 and later write it.
     *
     * The broker writes it in the version 0 layout whichever version was asked for, so it has no throttle time;
     * since KIP-511 the array is not empty but carries the single row of the ApiVersions api itself, which is how a
     * client that guessed too high learns which version it should have asked for.
     *
     *   Size          => 00 00 00 10 (16 bytes)
     *   CorrelationId => 00 00 00 65
     *   ErrorCode     => 00 23 (35, UnsupportedVersion)
     *   ApiVersions   => 00 00 00 01
     *     ApiKey/Min/Max => 00 12 00 00 00 03
     */
    private const string UNSUPPORTED_VERSION_RESPONSE_HEX = '00000010'
        . '00000065'
        . '0023'
        . '00000001'
        . '0012' . '0000' . '0003';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new ApiVersionsRequest('test', 1);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::API_VERSIONS, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion(), 'this client sends the version 2 of Kafka 2.0 (KIP-219)');
        self::assertSame(14, $request->getMessageSize(), 'header only, the request has no body');
    }

    /**
     * "Versions 0 through 2 of ApiVersionsRequest are the same", so the three frames differ in one header field
     *
     * @return array<string, array{class-string<ApiVersionsRequest>, int, string}>
     */
    public static function requestVersionProvider(): array
    {
        return [
            'v0, Kafka 0.10.0'          => [ApiVersionsRequestV0::class, 0, self::REQUEST_V0_HEX],
            'v1, Kafka 0.11 (KIP-124)'  => [ApiVersionsRequestV1::class, 1, self::REQUEST_V1_HEX],
            'v2, Kafka 2.0 (KIP-219)'   => [ApiVersionsRequest::class, 2, self::REQUEST_HEX],
        ];
    }

    /**
     * @param class-string<ApiVersionsRequest> $class
     */
    #[DataProvider('requestVersionProvider')]
    public function testEveryVersionOfTheRequestIsTheHeaderAndNothingElse(string $class, int $version, string $hex): void
    {
        $request = new $class('test', 1);

        self::assertSame($hex, bin2hex((string) $request));
        self::assertSame($version, $request->getApiVersion());
        self::assertSame($version, $class::VERSION);
        self::assertSame(14, $request->getMessageSize());
        self::assertSame(
            substr(self::REQUEST_HEX, 16),
            substr($hex, 16),
            'everything behind the api version is identical'
        );
    }

    public function testRequestWithoutAClientIdIsTwoBytesShorter(): void
    {
        $request = new ApiVersionsRequest();

        self::assertSame('0000000a' . '0012' . '0002' . '00000000' . '0000', bin2hex((string) $request));
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
        self::assertSame(9, $produce->maxVersion);

        $controlledShutdown = $response->apiVersions[ApiKeys::CONTROLLED_SHUTDOWN];
        self::assertSame(0, $controlledShutdown->minVersion, 'every minimum is 0 again since Kafka 1.0');
        self::assertSame(3, $controlledShutdown->maxVersion);
    }

    /**
     * The throttle time closes the answer, so a version 0 frame is the same bytes without the last four
     */
    public function testTheThrottleTimeIsAppendedToTheAnswerAndNotPrepended(): void
    {
        $versionZero = ApiVersionsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_V0_HEX)));
        $versionTwo  = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame($versionZero->getMessageSize() + 4, $versionTwo->getMessageSize());
        self::assertSame($versionZero->errorCode, $versionTwo->errorCode);
        self::assertSame(array_keys($versionZero->apiVersions), array_keys($versionTwo->apiVersions));
        self::assertSame(
            substr(self::RESPONSE_HEX, 16, -8),
            substr(self::RESPONSE_V0_HEX, 16),
            'the body of the two versions is identical up to the trailing throttle time'
        );
        self::assertSame(0, $versionZero->throttleTimeMs, 'the field defaults to 0 when the frame does not carry it');
    }

    /**
     * The KIP-219 bump of Kafka 2.0 adds no field: the version 1 and the version 2 answer are the same frame
     */
    public function testTheVersionTwoAnswerIsTheVersionOneFrame(): void
    {
        $versionOne = ApiVersionsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $versionTwo = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, ApiVersionsResponseV1::VERSION);
        self::assertSame(2, ApiVersionsResponse::VERSION);
        self::assertSame(
            ApiVersionsResponseV1::getScheme(),
            ApiVersionsResponse::getScheme(),
            'KIP-219 bumped the version without changing the schema'
        );
        self::assertSame(bin2hex((string) $versionOne), bin2hex((string) $versionTwo));
        self::assertSame($versionOne->throttleTimeMs, $versionTwo->throttleTimeMs);
    }

    public function testResponseIsRepackedByteForByte(): void
    {
        $versionTwo  = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $versionOne  = ApiVersionsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $versionZero = ApiVersionsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_V0_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $versionTwo));
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $versionOne));
        self::assertSame(self::RESPONSE_V0_HEX, bin2hex((string) $versionZero));
    }

    public function testSupportsAndMaxVersionAnswerFromTheRangeOfOneApi(): void
    {
        $response = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertTrue($response->supports(ApiKeys::PRODUCE, 0));
        self::assertTrue($response->supports(ApiKeys::PRODUCE, 9));
        self::assertFalse($response->supports(ApiKeys::PRODUCE, 10));
        self::assertTrue($response->supports(ApiKeys::CONTROLLED_SHUTDOWN, 0), 'the minimum version is inclusive');
        self::assertTrue($response->supports(ApiKeys::CONTROLLED_SHUTDOWN, 3));
        self::assertFalse($response->supports(ApiKeys::FETCH, 0), 'an api the broker does not report at all');

        self::assertSame(9, $response->maxVersionOf(ApiKeys::PRODUCE));
        self::assertSame(3, $response->maxVersionOf(ApiKeys::API_VERSIONS));
        self::assertNull($response->maxVersionOf(ApiKeys::FETCH));
    }

    /**
     * The 35 of a 2.4 or later broker carries the one row that says which version the client should have asked for
     */
    public function testAnUnsupportedVersionIsReportedWithTheErrorCodeAndTheApiVersionsRow(): void
    {
        $response = ApiVersionsResponseV0::unpack(
            new StringStream((string) hex2bin(self::UNSUPPORTED_VERSION_RESPONSE_HEX))
        );

        self::assertSame(101, $response->getCorrelationId());
        self::assertSame(KafkaException::UNSUPPORTED_VERSION, $response->errorCode);
        self::assertSame([ApiKeys::API_VERSIONS], array_keys($response->apiVersions), 'KIP-511');
        self::assertSame(3, $response->maxVersionOf(ApiKeys::API_VERSIONS));
        self::assertNull($response->maxVersionOf(ApiKeys::PRODUCE));
        self::assertSame(self::UNSUPPORTED_VERSION_RESPONSE_HEX, bin2hex((string) $response));
    }
}
