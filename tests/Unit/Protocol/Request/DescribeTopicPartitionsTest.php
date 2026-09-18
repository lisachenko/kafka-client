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
use Protocol\Kafka\Admin\TopicDescription;
use Protocol\Kafka\Admin\TopicPartitionInfo;
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsCursor;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsRequestTopic;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsResponseTopic;
use Protocol\Kafka\Protocol\Request\DescribeTopicPartitionsRequest;
use Protocol\Kafka\Protocol\Request\DescribeTopicPartitionsResponse;

/**
 * Byte-exact tests for DescribeTopicPartitions (key 75, v0), the api Kafka 3.8 added with KIP-966.
 *
 * The api that pages: the request bounds the answer with a `response_partition_limit` and may start it at a
 * `cursor`, and the answer hands the next cursor back. Both cursors are **nullable structures**, the shape this
 * protocol had not used before - one int8 in front of the structure, `-1` for null and `1` for present.
 *
 * @see docs/protocol/3.9.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
#[CoversClass(DescribeTopicPartitionsRequest::class)]
#[CoversClass(DescribeTopicPartitionsResponse::class)]
#[CoversClass(DescribeTopicPartitionsRequestTopic::class)]
#[CoversClass(DescribeTopicPartitionsResponseTopic::class)]
#[CoversClass(DescribeTopicPartitionsResponsePartition::class)]
#[CoversClass(DescribeTopicPartitionsCursor::class)]
#[CoversClass(TopicDescription::class)]
#[CoversClass(TopicPartitionInfo::class)]
final class DescribeTopicPartitionsTest extends TestCase
{
    /**
     * A request for one topic with the default limit and no cursor.
     *
     *   Size          => 00 00 00 19 (25 bytes)
     *   ApiKey        => 00 4b (75), ApiVersion => 00 00
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test", TAG_BUFFER => 00
     *   Topics        => 02 (1 + 1), 02 "t", TAG_BUFFER 00
     *   ResponsePartitionLimit => 00 00 07 d0 (2000)
     *   Cursor        => ff (null)
     *   TAG_BUFFER    => 00
     */
    private const string REQUEST_HEX = '00000019'
        . '004b'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '02' . '02' . '74' . '00'
        . '000007d0'
        . 'ff'
        . '00';

    /**
     * The same request with a cursor: seven bytes more, and the `ff` becomes the `01` of a structure.
     */
    private const string REQUEST_HEX_WITH_CURSOR = '00000020'
        . '004b'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '02' . '02' . '74' . '00'
        . '000007d0'
        . '01' . '02' . '74' . '00000002' . '00'
        . '00';

    /**
     * An answer with one topic of one partition and a `next_cursor` that points at the partition 1.
     *
     *   Size           => 00 00 00 4a (74 bytes)
     *   CorrelationId  => 00 00 00 09, TAG_BUFFER => 00
     *   ThrottleTimeMs => 00 00 00 00
     *   Topics         => 02 (1 + 1)
     *     ErrorCode 00 00, Name 02 "t", TopicId 16 bytes, IsInternal 00, Partitions 02 (1 + 1)
     *       ErrorCode 00 00, PartitionIndex 00 00 00 00, LeaderId 00 00 00 01, LeaderEpoch 00 00 00 05,
     *       ReplicaNodes 02 / 1, IsrNodes 02 / 1, EligibleLeaderReplicas 01 (empty), LastKnownElr 01 (empty),
     *       OfflineReplicas 01 (empty), TAG_BUFFER 00
     *     TopicAuthorizedOperations 00 00 0d f8 (3576), TAG_BUFFER 00
     *   NextCursor     => 01, 02 "t", 00 00 00 01, TAG_BUFFER 00
     *   TAG_BUFFER     => 00
     */
    private const string RESPONSE_HEX = '0000004a'
        . '00000009'
        . '00'
        . '00000000'
        . '02'
        . '0000'
        . '02' . '74'
        . '000102030405060708090a0b0c0d0e0f'
        . '00'
        . '02'
        . '0000'
        . '00000000'
        . '00000001'
        . '00000005'
        . '02' . '00000001'
        . '02' . '00000001'
        . '01'
        . '01'
        . '01'
        . '00'
        . '00000df8'
        . '00'
        . '01' . '02' . '74' . '00000001' . '00'
        . '00';

    public function testTheRequestCarriesTheTopicsTheLimitAndANullCursor(): void
    {
        $request = new DescribeTopicPartitionsRequest(['t'], 2000, null, 'test', 9);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_TOPIC_PARTITIONS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertTrue(DescribeTopicPartitionsRequest::isFlexible());
        self::assertSame(DescribeTopicPartitionsRequest::HEADER_V2, $request->getHeaderVersion());
        self::assertSame(['t'], $request->getTopics());
        self::assertSame(2000, $request->getResponsePartitionLimit());
        self::assertSame(
            DescribeTopicPartitionsRequest::DEFAULT_PARTITION_LIMIT,
            $request->getResponsePartitionLimit(),
            'the default of the specification'
        );
        self::assertNull($request->getCursor());
    }

    /**
     * A cursor costs the one byte that says it is there plus the structure, which ends in a section of its own
     */
    public function testACursorIsSevenBytesMoreThanANullOne(): void
    {
        $request = new DescribeTopicPartitionsRequest(
            ['t'],
            2000,
            new DescribeTopicPartitionsCursor('t', 2),
            'test',
            9
        );

        self::assertSame(self::REQUEST_HEX_WITH_CURSOR, bin2hex((string) $request));
        self::assertSame(
            strlen(self::REQUEST_HEX) / 2 + 7,
            strlen(self::REQUEST_HEX_WITH_CURSOR) / 2
        );
        self::assertSame('t', $request->getCursor()?->topicName);
        self::assertSame(2, $request->getCursor()?->partitionIndex);
    }

    /**
     * An empty topic array is a legal frame - and it is the request for EVERY topic of the cluster
     */
    public function testAnEmptyTopicArrayIsTheShortestFrameOfTheApi(): void
    {
        $request = new DescribeTopicPartitionsRequest([], 2000, null, 'test', 9);

        self::assertSame(
            '00000016' . '004b' . '0000' . '00000009' . '0004' . '74657374' . '00'
            . '01' . '000007d0' . 'ff' . '00',
            bin2hex((string) $request)
        );
        self::assertSame([], $request->getTopics());
    }

    public function testTheAnswerDecodesIntoTopicsPartitionsAndTheNextCursor(): void
    {
        $response = DescribeTopicPartitionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['t'], array_keys($response->topics), 'indexed by the topic name');

        $topic = $response->topics['t'];

        self::assertSame(0, $topic->errorCode);
        self::assertSame('t', $topic->name);
        self::assertSame('AAECAwQFBgcICQoLDA0ODw', Uuid::toString($topic->topicId));
        self::assertFalse($topic->isInternal);
        self::assertSame(3576, $topic->topicAuthorizedOperations);
        self::assertSame([0], array_keys($topic->partitions), 'indexed by the partition index');

        $partition = $topic->partitions[0];

        self::assertSame(0, $partition->errorCode);
        self::assertSame(1, $partition->leaderId);
        self::assertSame(5, $partition->leaderEpoch);
        self::assertSame([1], $partition->replicaNodes);
        self::assertSame([1], $partition->isrNodes);
        self::assertSame([], $partition->eligibleLeaderReplicas, 'the node writes an empty array, never a null');
        self::assertSame([], $partition->lastKnownElr);
        self::assertSame([], $partition->offlineReplicas);

        self::assertSame('t', $response->nextCursor?->topicName);
        self::assertSame(1, $response->nextCursor?->partitionIndex);
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The two ELR arrays are nullable, and the compact `00` of a null one is not the `01` of an empty one
     */
    public function testANullEligibleLeaderReplicaArrayIsNotAnEmptyOne(): void
    {
        $withNulls = str_replace(
            '02' . '00000001' . '02' . '00000001' . '01' . '01' . '01',
            '02' . '00000001' . '02' . '00000001' . '00' . '00' . '01',
            self::RESPONSE_HEX
        );
        $response  = DescribeTopicPartitionsResponse::unpack(new StringStream((string) hex2bin($withNulls)));
        $partition = $response->topics['t']->partitions[0];

        self::assertNull($partition->eligibleLeaderReplicas);
        self::assertNull($partition->lastKnownElr);
        self::assertSame($withNulls, bin2hex((string) $response), 'and a null is written back as a null');
    }

    /**
     * The answer of a listing that is complete carries no cursor at all
     */
    public function testAnAnswerWithoutANextCursorIsOneByteShorter(): void
    {
        $lastPage = str_replace(
            '01' . '02' . '74' . '00000001' . '00' . '00',
            'ff' . '00',
            self::RESPONSE_HEX
        );
        $lastPage = '00000043' . substr($lastPage, 8);

        $response = DescribeTopicPartitionsResponse::unpack(new StringStream((string) hex2bin($lastPage)));

        self::assertNull($response->nextCursor, 'a null next cursor is "the listing is complete"');
        self::assertSame($lastPage, bin2hex((string) $response));
    }

    /**
     * The value objects of the admin client carry what the entry of the answer holds
     */
    public function testTheValueObjectsMirrorTheAnswer(): void
    {
        $response  = DescribeTopicPartitionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $topic     = $response->topics['t'];
        $partition = TopicPartitionInfo::fromResponsePartition($topic->partitions[0]);

        self::assertSame(0, $partition->partition);
        self::assertSame(1, $partition->leader);
        self::assertTrue($partition->hasLeader());
        self::assertSame(5, $partition->leaderEpoch);
        self::assertSame([1], $partition->replicas);
        self::assertSame([1], $partition->isr);
        self::assertSame([], $partition->elr);
        self::assertSame([], $partition->lastKnownElr);
        self::assertSame([], $partition->offlineReplicas);

        $description = new TopicDescription(
            $topic->name ?? '',
            $topic->isInternal,
            [$partition->partition => $partition],
            $topic->topicAuthorizedOperations,
            $topic->topicId
        );

        self::assertSame('t', $description->name);
        self::assertFalse($description->internal);
        self::assertSame('AAECAwQFBgcICQoLDA0ODw', $description->topicIdAsString());
        self::assertSame(AclOperation::fromBitField(3576), $description->authorizedOperations());
        self::assertNotSame([], $description->authorizedOperations(), 'the api reports the bit field unasked');
    }

    /**
     * A partition without a leader is the -1 of the specification, which the value object knows about
     */
    public function testAPartitionWithoutALeaderIsReported(): void
    {
        $partition               = new DescribeTopicPartitionsResponsePartition();
        $partition->leaderId     = DescribeTopicPartitionsResponsePartition::NO_LEADER;
        $partition->partitionIndex = 7;

        $info = TopicPartitionInfo::fromResponsePartition($partition);

        self::assertSame(7, $info->partition);
        self::assertSame(TopicPartitionInfo::NO_LEADER, $info->leader);
        self::assertFalse($info->hasLeader());
    }
}
