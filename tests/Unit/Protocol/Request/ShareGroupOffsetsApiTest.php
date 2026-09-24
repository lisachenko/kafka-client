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
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\AlterShareGroupOffsetsRequestPartition;
use Protocol\Kafka\Protocol\Data\AlterShareGroupOffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\AlterShareGroupOffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\AlterShareGroupOffsetsResponseTopic;
use Protocol\Kafka\Protocol\Data\DeleteShareGroupOffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\DeleteShareGroupOffsetsResponseTopic;
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsRequestGroup;
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponseGroup;
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponseTopic;
use Protocol\Kafka\Protocol\Request\AlterShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\AlterShareGroupOffsetsResponse;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupOffsetsResponse;
use Protocol\Kafka\Protocol\Request\DescribeShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\DescribeShareGroupOffsetsResponse;

/**
 * Byte-exact tests for the share-group offset apis of Kafka 4.1 (KIP-932, api keys 90 to 92, v0 each).
 *
 * @see docs/protocol/4.3.md, sections "DescribeShareGroupOffsets API (key 90, v0)", "AlterShareGroupOffsets API (key
 *      91, v0)" and "DeleteShareGroupOffsets API (key 92, v0)"
 */
#[CoversClass(DescribeShareGroupOffsetsRequest::class)]
#[CoversClass(DescribeShareGroupOffsetsResponse::class)]
#[CoversClass(DescribeShareGroupOffsetsRequestGroup::class)]
#[CoversClass(DescribeShareGroupOffsetsRequestTopic::class)]
#[CoversClass(DescribeShareGroupOffsetsResponseGroup::class)]
#[CoversClass(DescribeShareGroupOffsetsResponseTopic::class)]
#[CoversClass(DescribeShareGroupOffsetsResponsePartition::class)]
#[CoversClass(AlterShareGroupOffsetsRequest::class)]
#[CoversClass(AlterShareGroupOffsetsResponse::class)]
#[CoversClass(AlterShareGroupOffsetsRequestTopic::class)]
#[CoversClass(AlterShareGroupOffsetsRequestPartition::class)]
#[CoversClass(AlterShareGroupOffsetsResponseTopic::class)]
#[CoversClass(AlterShareGroupOffsetsResponsePartition::class)]
#[CoversClass(DeleteShareGroupOffsetsRequest::class)]
#[CoversClass(DeleteShareGroupOffsetsResponse::class)]
#[CoversClass(DeleteShareGroupOffsetsRequestTopic::class)]
#[CoversClass(DeleteShareGroupOffsetsResponseTopic::class)]
final class ShareGroupOffsetsApiTest extends TestCase
{
    /**
     * The rest of the request header v2 of every request of this class: correlation id 7, client id "test", no
     * tagged field
     */
    private const string HEADER_TAIL = '00000007' . '000474657374' . '00';

    /**
     * The group id "g" as a compact string
     */
    private const string GROUP = '0267';

    /**
     * The topic name "t" as a compact string
     */
    private const string TOPIC = '0274';

    public function testTheThreeApisAreFlexibleFromTheirVersionZero(): void
    {
        foreach ([
            DescribeShareGroupOffsetsRequest::class => ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS,
            AlterShareGroupOffsetsRequest::class    => ApiKeys::ALTER_SHARE_GROUP_OFFSETS,
            DeleteShareGroupOffsetsRequest::class   => ApiKeys::DELETE_SHARE_GROUP_OFFSETS,
        ] as $class => $apiKey) {
            self::assertSame($apiKey, $class::API_KEY);
            self::assertSame(0, $class::VERSION, 'the version 0 of Kafka 4.1');
            self::assertTrue($class::isFlexible());
        }
        self::assertSame([90, 91, 92], [
            ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS,
            ApiKeys::ALTER_SHARE_GROUP_OFFSETS,
            ApiKeys::DELETE_SHARE_GROUP_OFFSETS,
        ]);
    }

    /**
     * A null topic array asks for every partition of the group, an empty one for none - two different bytes
     */
    public function testTheTopicsOfADescribedGroupAreNullableAndNullIsNotEmpty(): void
    {
        $all   = new DescribeShareGroupOffsetsRequest(['g' => null], 'test', 7);
        $none  = new DescribeShareGroupOffsetsRequest(['g' => []], 'test', 7);
        $named = new DescribeShareGroupOffsetsRequest(['g' => ['t' => [0, 2]]], 'test', 7);

        $request = '005a' . '0000' . self::HEADER_TAIL . '02' . self::GROUP;
        self::assertSame(self::frame($request . '00' . '00' . '00'), bin2hex((string) $all));
        self::assertSame(self::frame($request . '01' . '00' . '00'), bin2hex((string) $none));
        self::assertSame(
            self::frame($request . '02' . self::TOPIC . '03' . '00000000' . '00000002' . '00' . '00' . '00'),
            bin2hex((string) $named)
        );
        self::assertNull($all->getGroups()['g']->topics);
        self::assertSame([], $none->getGroups()['g']->topics);
    }

    public function testTheDescribeAnswerCarriesTheStartOffsetsAndTheErrorsOfAGroupBehindItsTopics(): void
    {
        $topicId = str_repeat("\x11", 16);
        $hex     = self::frame(
            '00000007' . '00' . '00000000'
            . '02' . self::GROUP
            . '02' . self::TOPIC . bin2hex($topicId)
            . '02' . '00000000' . '0000000000000003' . '00000000' . '0000' . '00' . '00'
            . '00'
            . '0000' . '00' . '00'
            . '00'
        );
        $answer  = DescribeShareGroupOffsetsResponse::unpack(new StringStream((string) hex2bin($hex)));

        $group     = $answer->groups['g'];
        $partition = $group->topics['t']->partitions[0];
        self::assertSame(KafkaException::NO_ERROR, $group->errorCode);
        self::assertSame($topicId, $group->topics['t']->topicId);
        self::assertSame(3, $partition->startOffset);
        self::assertSame(0, $partition->leaderEpoch);
        self::assertNull($partition->errorMessage);
        self::assertSame($hex, bin2hex((string) $answer));
    }

    public function testTheAlterRequestNamesTheStartOffsetOfEveryPartition(): void
    {
        $request = new AlterShareGroupOffsetsRequest('g', ['t' => [0 => 3, 1 => 0]], 'test', 7);

        self::assertSame('g', $request->getGroupId());
        self::assertSame(
            self::frame(
                '005b' . '0000' . self::HEADER_TAIL . self::GROUP
                . '02' . self::TOPIC . '03'
                . '00000000' . '0000000000000003' . '00'
                . '00000001' . '0000000000000000' . '00'
                . '00' . '00'
            ),
            bin2hex((string) $request)
        );
    }

    public function testTheAlterAnswerHasATopLevelErrorAndOneCodePerPartition(): void
    {
        $hex    = self::frame(
            '00000007' . '00' . '00000000' . '0044' . '04747874'
            . '02' . self::TOPIC . bin2hex(Uuid::ZERO) . '02' . '00000000' . '0003' . '00' . '00' . '00' . '00'
        );
        $answer = AlterShareGroupOffsetsResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(68, $answer->errorCode, 'the NonEmptyGroup of a group with members');
        self::assertSame('txt', $answer->errorMessage);
        self::assertSame(3, $answer->responses['t']->partitions[0]->errorCode);
        self::assertNull($answer->responses['t']->partitions[0]->errorMessage);
        self::assertSame($hex, bin2hex((string) $answer));
    }

    public function testTheDeleteRequestNamesWholeTopics(): void
    {
        $request = new DeleteShareGroupOffsetsRequest('g', ['a', 'b'], 'test', 7);

        self::assertSame('g', $request->getGroupId());
        self::assertSame(
            self::frame('005c' . '0000' . self::HEADER_TAIL . self::GROUP . '03' . '026100' . '026200' . '00'),
            bin2hex((string) $request)
        );
    }

    public function testTheDeleteAnswerCarriesOneCodePerTopic(): void
    {
        $hex    = self::frame(
            '00000007' . '00' . '00000000' . '0000' . '00'
            . '02' . self::TOPIC . bin2hex(Uuid::ZERO) . '0003' . '00' . '00' . '00'
        );
        $answer = DeleteShareGroupOffsetsResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertNull($answer->errorMessage);
        self::assertSame(3, $answer->responses['t']->errorCode);
        self::assertSame(Uuid::ZERO, $answer->responses['t']->topicId);
        self::assertSame($hex, bin2hex((string) $answer));
    }

    /**
     * Puts the size field in front of the hex dump of a frame
     */
    private static function frame(string $hex): string
    {
        return sprintf('%08x', strlen($hex) / 2) . $hex;
    }
}
