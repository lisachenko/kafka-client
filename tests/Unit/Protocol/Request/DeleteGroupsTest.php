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
use Protocol\Kafka\Protocol\Request\DeleteGroupsResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;

/**
 * Byte-exact tests for the DeleteGroups API of Kafka 1.1 (api key 42, v0, KIP-229).
 *
 * @see docs/protocol/1.1.md, section "DeleteGroups API (key 42, v0)"
 */
#[CoversClass(DeleteGroupsRequest::class)]
#[CoversClass(DeleteGroupsResponse::class)]
#[CoversClass(DeleteGroupsResponseGroup::class)]
final class DeleteGroupsTest extends TestCase
{
    /**
     * DeleteGroups request v0 for two groups.
     *
     *   Size          => 00 00 00 24 (36 bytes)
     *   ApiKey        => 00 2a (42)
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test"
     *   Groups        => 00 00 00 02, 00 07 "group-a", 00 07 "group-b"
     */
    private const string REQUEST_HEX = '00000024'
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
        self::assertSame(0, $request->getApiVersion(), 'a 1.1.1 broker only serves version 0');
        self::assertSame(['group-a', 'group-b'], $request->getGroups());
    }

    public function testTheFrameIsTheOneOfDescribeGroups(): void
    {
        // KIP-229 gave the new api the request of DescribeGroups: the group array and nothing else
        $delete   = new DeleteGroupsRequest(['group-a'], 'test', 9);
        $describe = new DescribeGroupsRequest(['group-a'], 'test', 9);

        self::assertSame(
            substr((string) $describe, 8),
            substr((string) $delete, 8),
            'behind the api key and the version of the header the two frames are the same bytes'
        );
        self::assertSame(strlen((string) $describe), strlen((string) $delete));
    }

    public function testAnEmptyGroupArrayIsALegalFrame(): void
    {
        $request = new DeleteGroupsRequest([], 'test', 9);

        self::assertStringEndsWith('00000000', bin2hex((string) $request));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DeleteGroupsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

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

    public function testResponseSurvivesARoundTrip(): void
    {
        $response = DeleteGroupsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }
}
