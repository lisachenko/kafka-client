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
use Protocol\Kafka\Protocol\Data\DeleteGroupsResponseGroup;
use Protocol\Kafka\Protocol\Request\DeleteGroupsRequest;
use Protocol\Kafka\Protocol\Request\DeleteGroupsRequestV0;
use Protocol\Kafka\Protocol\Request\DeleteGroupsRequestV1;
use Protocol\Kafka\Protocol\Request\DeleteGroupsResponse;
use Protocol\Kafka\Protocol\Request\DeleteGroupsResponseV0;
use Protocol\Kafka\Protocol\Request\DeleteGroupsResponseV1;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV2;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV3;

/**
 * Byte-exact tests for the DeleteGroups API of Kafka 1.1 (api key 42, v0, KIP-229).
 *
 * @see docs/protocol/2.8.md, section "DeleteGroups API (key 42, v0 to v2)"
 */
#[CoversClass(DeleteGroupsRequest::class)]
#[CoversClass(DeleteGroupsRequestV0::class)]
#[CoversClass(DeleteGroupsRequestV1::class)]
#[CoversClass(DeleteGroupsResponse::class)]
#[CoversClass(DeleteGroupsResponseV0::class)]
#[CoversClass(DeleteGroupsResponseV1::class)]
#[CoversClass(DeleteGroupsResponseGroup::class)]
final class DeleteGroupsTest extends TestCase
{
    /**
     * DeleteGroups request v2 (Kafka 2.4, KIP-482) for two groups, the flexible frame this client sends.
     *
     *   Size          => 00 00 00 21 (33 bytes)
     *   ApiKey        => 00 2a (42)
     *   ApiVersion    => 00 02
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test" (never compact)
     *   TAG_BUFFER    => 00 (of the request header v2)
     *   Groups        => 03 (compact: 2 + 1), 08 "group-a", 08 "group-b"
     *   TAG_BUFFER    => 00 (of the body)
     */
    private const string REQUEST_HEX = '00000021'
        . '002a'
        . '0002'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '03' . '08' . '67726f75702d61' . '08' . '67726f75702d62'
        . '00';

    /**
     * The same request as a version 1 frame, plainly encoded.
     */
    private const string REQUEST_V1_HEX = '00000024'
        . '002a'
        . '0001'
        . '00000009'
        . '0004' . '74657374'
        . '00000002' . '0007' . '67726f75702d61' . '0007' . '67726f75702d62';

    /**
     * The very same body as a version 0 frame, which is the only version a 1.1.1 broker serves.
     */
    private const string REQUEST_V0_HEX = '00000024'
        . '002a'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00000002' . '0007' . '67726f75702d61' . '0007' . '67726f75702d62';

    /**
     * DeleteGroups response v0 with the three answers of KIP-229: deleted, not empty, unknown.
     *
     *   Size            => 00 00 00 2d (45 bytes)
     *   CorrelationId   => 00 00 00 09
     *   ThrottleTimeMs  => 00 00 00 0c (12)
     *   GroupErrorCodes => 00 00 00 03
     *     00 07 "group-a", 00 00 (0)
     *     00 07 "group-b", 00 44 (68, NonEmptyGroup)
     *     00 07 "group-c", 00 45 (69, GroupIdNotFound)
     */
    private const string RESPONSE_HEX = '0000002d'
        . '00000009'
        . '0000000c'
        . '00000003'
        . '0007' . '67726f75702d61' . '0000'
        . '0007' . '67726f75702d62' . '0044'
        . '0007' . '67726f75702d63' . '0045';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DeleteGroupsRequest(['group-a', 'group-b'], 'test', 9);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DELETE_GROUPS, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion(), 'KIP-482 makes the version this client sends 2');
        self::assertSame(['group-a', 'group-b'], $request->getGroups());
        self::assertTrue(DeleteGroupsRequest::isFlexible());
    }

    public function testTheVersionOneRequestIsThePlainEncodingOfTheSameGroups(): void
    {
        $request = new DeleteGroupsRequestV1(['group-a', 'group-b'], 'test', 9);

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
        self::assertFalse(DeleteGroupsRequestV1::isFlexible());
    }

    public function testTheVersionZeroRequestIsTheSameBody(): void
    {
        $request = new DeleteGroupsRequestV0(['group-a', 'group-b'], 'test', 9);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion(), 'the only version a 1.1.1 broker serves');
        self::assertSame(
            substr(self::REQUEST_V1_HEX, 16),
            substr(self::REQUEST_V0_HEX, 16),
            'KIP-219 raised the api version of DeleteGroups without adding a field'
        );
    }

    public function testTheFrameIsTheOneOfDescribeGroups(): void
    {
        // KIP-229 gave the new api the request of DescribeGroups: the group array and nothing else. The two frames
        // only parted ways at DescribeGroups v3 (KIP-430), which appended the `include_authorized_operations`
        // flag to that api alone, so the comparison is against the version 2 of it.
        $delete   = new DeleteGroupsRequestV1(['group-a'], 'test', 9);
        $describe = new DescribeGroupsRequestV2(['group-a'], 'test', 9);

        self::assertSame(
            substr((string) $describe, 8),
            substr((string) $delete, 8),
            'behind the api key and the version of the header the two frames are the same bytes'
        );
        self::assertSame(strlen((string) $describe), strlen((string) $delete));
        self::assertSame(
            strlen((string) $delete) + 1,
            strlen((string) new DescribeGroupsRequestV3(['group-a'], 'test', 9)),
            'the one byte the version 3 of DescribeGroups added is the whole difference'
        );
    }

    public function testAnEmptyGroupArrayIsALegalFrame(): void
    {
        $request = new DeleteGroupsRequest([], 'test', 9);

        self::assertStringEndsWith(
            '0100',
            bin2hex((string) $request),
            'an empty compact array is the unsigned varint 1, and the body still ends in its tag buffer'
        );
        self::assertStringEndsWith('00000000', bin2hex((string) new DeleteGroupsRequestV1([], 'test', 9)));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DeleteGroupsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(9, $response->getCorrelationId());
        self::assertSame(12, $response->throttleTimeMs);
        self::assertSame(['group-a', 'group-b', 'group-c'], array_keys($response->groups));

        self::assertSame(KafkaException::NO_ERROR, $response->groups['group-a']->errorCode);
        self::assertSame(
            KafkaException::NON_EMPTY_GROUP,
            $response->groups['group-b']->errorCode,
            'a group that still has a member keeps its offsets'
        );
        self::assertSame(
            KafkaException::GROUP_ID_NOT_FOUND,
            $response->groups['group-c']->errorCode,
            'and one the coordinator never heard of says so with a code of its own'
        );
    }

    public function testTheVersionZeroAnswerIsTheVersionOneAnswer(): void
    {
        $response = DeleteGroupsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(12, $response->throttleTimeMs);
        self::assertSame(['group-a', 'group-b', 'group-c'], array_keys($response->groups));
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $response = DeleteGroupsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The version 2 answer is the same entries compactly, each closed by its own tagged-field section
     */
    public function testTheVersionTwoAnswerIsTheFlexibleEncodingOfTheSameEntries(): void
    {
        $frame = '0000002c'
            . '00000009'
            . '00'
            . '0000000c'
            . '04'
            . '08' . '67726f75702d61' . '0000' . '00'
            . '08' . '67726f75702d62' . '0044' . '00'
            . '08' . '67726f75702d63' . '0045' . '00'
            . '00';

        $response = DeleteGroupsResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(12, $response->throttleTimeMs);
        self::assertSame(['group-a', 'group-b', 'group-c'], array_keys($response->groups));
        self::assertSame(KafkaException::NON_EMPTY_GROUP, $response->groups['group-b']->errorCode);
        self::assertSame($frame, bin2hex((string) $response), 'the answer survives the round trip');
    }
}
