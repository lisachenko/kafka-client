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
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV2;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV1;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV2;

/**
 * Byte-exact tests for the ApiVersions API (api key 18, v0 to v3).
 *
 * Kafka 0.10.0 added version 0, Kafka 0.11 version 1 - the same request with a `throttle_time_ms` appended to the
 * answer, the one api of that release that puts the field at the end instead of the beginning - Kafka 2.0 the
 * version 2 of KIP-219, whose frame is the frame of version 1 and whose meaning is that the client waits out a
 * throttle time itself, and Kafka 2.4 the version 3: the **first flexible frame** of the protocol, with a request
 * header v2, two compact strings of KIP-511 in the request, a compact api array in the answer and the tagged
 * fields of KIP-584 at the end of it - behind a response header **v0**, which is the exception this api is.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
 */
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsRequestV0::class)]
#[CoversClass(ApiVersionsRequestV1::class)]
#[CoversClass(ApiVersionsRequestV2::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseV0::class)]
#[CoversClass(ApiVersionsResponseV1::class)]
#[CoversClass(ApiVersionsResponseV2::class)]
#[CoversClass(ApiVersionsResponseMetadata::class)]
final class ApiVersionsTest extends TestCase
{
    /**
     * ApiVersions request v3, the version this client sends: the first flexible frame of the protocol.
     *
     *   Size                  => 00 00 00 2c (44 bytes)
     *   ApiKey                => 00 12 (18)
     *   ApiVersion            => 00 03
     *   CorrelationId         => 00 00 00 01
     *   ClientId              => 00 04 "test"           (int16 length even here: "flexibleVersions": "none")
     *   TAG_BUFFER            => 00                     (the tagged fields of the request header v2)
     *   ClientSoftwareName    => 18 "lisachenko-kafka-client"   (compact: 23 + 1)
     *   ClientSoftwareVersion => 04 "2.8"                       (compact: 3 + 1)
     *   TAG_BUFFER            => 00                     (the tagged fields of the body)
     */
    private const string REQUEST_HEX = '0000002c'
        . '0012'
        . '0003'
        . '00000001'
        . '0004' . '74657374'
        . '00'
        . '18' . '6c6973616368656e6b6f2d6b61666b612d636c69656e74'
        . '04' . '322e38'
        . '00';

    /**
     * The same request as version 2, which has no body at all and the plain request header.
     */
    private const string REQUEST_V2_HEX = '0000000e'
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
    private const string RESPONSE_V2_HEX = '00000020'
        . '00000001'
        . '0000'
        . '00000003'
        . '0000' . '0000' . '0009'
        . '0007' . '0000' . '0003'
        . '0012' . '0000' . '0003'
        . '00000000';

    /**
     * The same three apis as an ApiVersions **v3** answer: compact, tagged, and behind a response header v0.
     *
     *   Size            => 00 00 00 2b (43 bytes)
     *   CorrelationId   => 00 00 00 01   (and no tag buffer: this api keeps the response header v0)
     *   ErrorCode       => 00 00
     *   ApiKeys         => 04            (compact array: 3 + 1)
     *     00 00 00 00 00 09 00           (Produce v0-v9, and the tag buffer of the entry)
     *     00 07 00 00 00 03 00
     *     00 12 00 00 00 03 00
     *   ThrottleTimeMs  => 00 00 00 00
     *   TAG_BUFFER      => 01            (one tagged field)
     *     01 08 00 00 00 00 00 00 00 00  (tag 1: finalized_features_epoch = 0, eight bytes long)
     */
    private const string RESPONSE_HEX = '0000002b'
        . '00000001'
        . '0000'
        . '04'
        . '000000000009' . '00'
        . '000700000003' . '00'
        . '001200000003' . '00'
        . '00000000'
        . '01' . '01' . '08' . '0000000000000000';

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
        self::assertSame(3, $request->getApiVersion(), 'this client sends the version 3 of Kafka 2.4');
        self::assertSame(44, $request->getMessageSize(), 'the header v2, the two compact strings and two tag buffers');
        self::assertTrue(ApiVersionsRequest::isFlexible(), 'version 3 is the first flexible version of this api');
        self::assertSame(ApiVersionsRequest::HEADER_V2, ApiVersionsRequest::getHeaderVersion());
    }

    /**
     * The broker refuses a client software name or version that does not match its pattern with the error code 42
     *
     * `ApiVersionsRequest.isValid()` @ 2.8.2 matches both strings against this regular expression and
     * `KafkaApis.handleApiVersionsRequest` answers `Errors.INVALID_REQUEST` when either fails, which is measured on
     * the container and stored as the vector `apiversions.response.v3.invalid-software-name`.
     */
    public function testTheClientSoftwareNameAndVersionMatchTheBrokerPattern(): void
    {
        $pattern = '/^[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?$/';

        self::assertMatchesRegularExpression($pattern, ApiVersionsRequest::CLIENT_SOFTWARE_NAME);
        self::assertMatchesRegularExpression($pattern, ApiVersionsRequest::CLIENT_SOFTWARE_VERSION);
    }

    /**
     * "Versions 0 through 2 of ApiVersionsRequest are the same", so the three frames differ in one header field
     *
     * @return array<string, array{class-string<ApiVersionsRequest>, int, string}>
     */
    public static function requestVersionProvider(): array
    {
        return [
            'v0, Kafka 0.10.0'         => [ApiVersionsRequestV0::class, 0, self::REQUEST_V0_HEX],
            'v1, Kafka 0.11 (KIP-124)' => [ApiVersionsRequestV1::class, 1, self::REQUEST_V1_HEX],
            'v2, Kafka 2.0 (KIP-219)'  => [ApiVersionsRequestV2::class, 2, self::REQUEST_V2_HEX],
        ];
    }

    /**
     * @param class-string<ApiVersionsRequest> $class
     */
    #[DataProvider('requestVersionProvider')]
    public function testEveryVersionBelowThreeIsTheHeaderAndNothingElse(string $class, int $version, string $hex): void
    {
        $request = new $class('test', 1);

        self::assertSame($hex, bin2hex((string) $request));
        self::assertSame($version, $request->getApiVersion());
        self::assertSame($version, $class::VERSION);
        self::assertSame(14, $request->getMessageSize());
        self::assertFalse($class::isFlexible(), 'the flexible versions of this api start at 3');
        self::assertSame(ApiVersionsRequest::HEADER_V1, $class::getHeaderVersion());
        self::assertSame(
            substr(self::REQUEST_V2_HEX, 16),
            substr($hex, 16),
            'everything behind the api version is identical'
        );
    }

    public function testRequestWithoutAClientIdIsFourBytesShorter(): void
    {
        $request = new ApiVersionsRequest();

        self::assertSame(
            '00000028' . '0012' . '0003' . '00000000' . '0000' . '00'
            . '18' . '6c6973616368656e6b6f2d6b61666b612d636c69656e74' . '04' . '322e38' . '00',
            bin2hex((string) $request)
        );
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
     * The three tagged fields of KIP-584, of which a ZooKeeper-backed broker answers exactly one
     */
    public function testTheTaggedFieldsOfTheVersionThreeAnswerAreTheFeaturesOfTheBroker(): void
    {
        $response = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame([], $response->supportedFeatures, 'tag 0 is absent: the broker declares no feature');
        self::assertSame([], $response->finalizedFeatures, 'tag 2 is absent as well');
        self::assertSame(
            0,
            $response->finalizedFeaturesEpoch,
            'tag 1 is present with the value 0, which is why it is on the wire at all: its default is -1'
        );
    }

    /**
     * The version 3 answer is the version 2 answer in the compact encoding, behind the **same** response header
     *
     * `ApiKeys.responseHeaderVersion()` @ 2.8.2 answers 0 for this api whatever the version is, so the correlation
     * id is followed by the error code directly and not by the tag buffer that every other flexible answer carries.
     */
    public function testTheVersionThreeAnswerIsCompactButKeepsTheResponseHeaderOfVersionZero(): void
    {
        $versionThree = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $versionTwo   = ApiVersionsResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(ApiVersionsResponse::HEADER_V0, ApiVersionsResponse::getHeaderVersion());
        self::assertTrue(ApiVersionsResponse::isFlexible());
        self::assertSame('0000', substr(self::RESPONSE_HEX, 16, 4), 'the error code follows the correlation id');

        // The same three apis, 11 bytes shorter: the int32 count is one byte, the four length prefixes are gone and
        // four tag buffers are there instead
        self::assertSame(array_keys($versionTwo->apiVersions), array_keys($versionThree->apiVersions));
        self::assertSame(43, $versionThree->getMessageSize());
        self::assertSame(32, $versionTwo->getMessageSize());
    }

    /**
     * The throttle time closes the answer, so a version 0 frame is the same bytes without the last four
     */
    public function testTheThrottleTimeIsAppendedToTheAnswerAndNotPrepended(): void
    {
        $versionZero = ApiVersionsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_V0_HEX)));
        $versionTwo  = ApiVersionsResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame($versionZero->getMessageSize() + 4, $versionTwo->getMessageSize());
        self::assertSame($versionZero->errorCode, $versionTwo->errorCode);
        self::assertSame(array_keys($versionZero->apiVersions), array_keys($versionTwo->apiVersions));
        self::assertSame(
            substr(self::RESPONSE_V2_HEX, 16, -8),
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
        $versionOne = ApiVersionsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));
        $versionTwo = ApiVersionsResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(1, ApiVersionsResponseV1::VERSION);
        self::assertSame(2, ApiVersionsResponseV2::VERSION);
        self::assertSame(
            ApiVersionsResponseV1::getScheme(),
            ApiVersionsResponseV2::getScheme(),
            'KIP-219 bumped the version without changing the schema'
        );
        self::assertSame(bin2hex((string) $versionOne), bin2hex((string) $versionTwo));
        self::assertSame($versionOne->throttleTimeMs, $versionTwo->throttleTimeMs);
    }

    public function testResponseIsRepackedByteForByte(): void
    {
        $versionThree = ApiVersionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $versionTwo   = ApiVersionsResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));
        $versionOne   = ApiVersionsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));
        $versionZero  = ApiVersionsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_V0_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $versionThree));
        self::assertSame(self::RESPONSE_V2_HEX, bin2hex((string) $versionTwo));
        self::assertSame(self::RESPONSE_V2_HEX, bin2hex((string) $versionOne));
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
