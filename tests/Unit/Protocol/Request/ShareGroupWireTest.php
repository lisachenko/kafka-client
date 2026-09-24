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
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgeRequestPartition;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgeRequestTopic;
use Protocol\Kafka\Protocol\Data\ShareFetchRequestPartition;
use Protocol\Kafka\Protocol\Data\ShareFetchRequestTopic;
use Protocol\Kafka\Protocol\Data\ShareFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\ShareGroupDescribeAssignment;
use Protocol\Kafka\Protocol\Data\ShareGroupHeartbeatAssignment;
use Protocol\Kafka\Protocol\Data\ShareLeaderIdAndEpoch;
use Protocol\Kafka\Protocol\Request\ShareAcknowledgeRequest;
use Protocol\Kafka\Protocol\Request\ShareAcknowledgeRequestV1;
use Protocol\Kafka\Protocol\Request\ShareAcknowledgeResponse;
use Protocol\Kafka\Protocol\Request\ShareAcknowledgeResponseV1;
use Protocol\Kafka\Protocol\Request\ShareFetchRequest;
use Protocol\Kafka\Protocol\Request\ShareFetchRequestV1;
use Protocol\Kafka\Protocol\Request\ShareFetchResponse;
use Protocol\Kafka\Protocol\Request\ShareFetchResponseV1;
use Protocol\Kafka\Protocol\Request\ShareGroupDescribeRequest;
use Protocol\Kafka\Protocol\Request\ShareGroupDescribeResponse;
use Protocol\Kafka\Protocol\Request\ShareGroupHeartbeatRequest;
use Protocol\Kafka\Protocol\Request\ShareGroupHeartbeatResponse;
use Protocol\Kafka\Tests\Compliance\VectorFile;

/**
 * The share-group wire of KIP-932 at version 1 (Kafka 4.1): ShareGroupHeartbeat (76), ShareGroupDescribe (77),
 * ShareFetch (78) and ShareAcknowledge (79).
 *
 * The frames the 4.3.1 node answered are the vectors of `share-group-heartbeat.json`, `share-group-describe.json`,
 * `share-fetch.json` and `share-acknowledge.json`, replayed by the compliance suite; this class pins what the classes
 * add around them: the versions, the order of the fields, the named constructors and the helpers that build the
 * topics of a request and read the acquired records of an answer.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupHeartbeat API (key 76, v1)"
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1 and v2)"
 */
#[CoversClass(ShareGroupHeartbeatRequest::class)]
#[CoversClass(ShareGroupHeartbeatResponse::class)]
#[CoversClass(ShareGroupDescribeRequest::class)]
#[CoversClass(ShareGroupDescribeResponse::class)]
#[CoversClass(ShareFetchRequest::class)]
#[CoversClass(ShareFetchResponse::class)]
#[CoversClass(ShareAcknowledgeRequest::class)]
#[CoversClass(ShareAcknowledgeResponse::class)]
#[CoversClass(ShareAcknowledgementBatch::class)]
#[CoversClass(ShareFetchResponsePartition::class)]
#[CoversClass(ShareGroupHeartbeatAssignment::class)]
#[CoversClass(ShareGroupDescribeAssignment::class)]
#[CoversClass(ShareLeaderIdAndEpoch::class)]
final class ShareGroupWireTest extends TestCase
{
    /**
     * The four apis start at version 1 - the version 0 of the 4.0 early access is gone - and are flexible throughout
     */
    public function testTheFourApisSpeakTheirVersionsAndAreFlexibleFromTheFirstVersion(): void
    {
        $classes = [
            [ApiKeys::SHARE_GROUP_HEARTBEAT, 1, ShareGroupHeartbeatRequest::class, ShareGroupHeartbeatResponse::class],
            [ApiKeys::SHARE_GROUP_DESCRIBE, 1, ShareGroupDescribeRequest::class, ShareGroupDescribeResponse::class],
            [ApiKeys::SHARE_FETCH, 2, ShareFetchRequest::class, ShareFetchResponse::class],
            [ApiKeys::SHARE_FETCH, 1, ShareFetchRequestV1::class, ShareFetchResponseV1::class],
            [ApiKeys::SHARE_ACKNOWLEDGE, 2, ShareAcknowledgeRequest::class, ShareAcknowledgeResponse::class],
            [ApiKeys::SHARE_ACKNOWLEDGE, 1, ShareAcknowledgeRequestV1::class, ShareAcknowledgeResponseV1::class],
        ];

        foreach ($classes as [$apiKey, $version, $request, $response]) {
            self::assertSame($apiKey, $request::API_KEY);
            self::assertSame($version, $request::VERSION, "{$request} speaks version {$version}");
            self::assertSame($version, $response::VERSION);
            self::assertSame(0, $request::FLEXIBLE_VERSION, 'flexible from the first version');
            self::assertSame(0, $response::FLEXIBLE_VERSION);
        }
    }

    /**
     * The heartbeat is five fields; the join carries the epoch 0 and the topics, the steady state two nulls
     */
    public function testTheHeartbeatsOfAShareMemberAreTheJoinTheSteadyStateAndTheLeave(): void
    {
        self::assertSame(
            ['groupId', 'memberId', 'memberEpoch', 'rackId', 'subscribedTopicNames'],
            array_slice(array_keys(ShareGroupHeartbeatRequest::getScheme()), -5)
        );

        $join = ShareGroupHeartbeatRequest::forJoin('g', 'm', ['t']);
        self::assertSame(ShareGroupHeartbeatRequest::JOIN_MEMBER_EPOCH, $join->getMemberEpoch());
        self::assertSame(['t'], $join->getSubscribedTopicNames());

        $steady = ShareGroupHeartbeatRequest::forHeartbeat('g', 'm', 3);
        self::assertSame(3, $steady->getMemberEpoch());
        self::assertNull($steady->getSubscribedTopicNames(), 'null is "unchanged"');

        // Size ApiKey Version Correlation ClientId(int16) tags | group member epoch rack(null) topics(null) tags
        self::assertSame(
            '00000016' . '004c' . '0001' . '00000000' . '0000' . '00'
            . '0267' . '026d' . 'ffffffff' . '00' . '00' . '00',
            bin2hex((string) ShareGroupHeartbeatRequest::forLeave('g', 'm')),
            'the leave is the epoch -1 and two nulls'
        );
    }

    /**
     * The assignment of the heartbeat names topics by id, the one of the describe by id and name
     */
    public function testTheAssignmentsAreReadByTopicIdAndByTopicName(): void
    {
        $answer = ShareGroupHeartbeatResponse::unpack(new StringStream(self::vectorBytes(
            'share-group-heartbeat',
            'sharegroupheartbeat.v1.assignment.response'
        )));
        self::assertNotNull($answer->assignment);
        $byId = $answer->assignment->partitionsByTopicId();
        self::assertCount(1, $byId);
        self::assertSame([0], array_values($byId)[0]);
        self::assertSame(16, strlen((string) array_key_first($byId)), 'the 16 raw bytes of the topic id');

        $described = ShareGroupDescribeResponse::unpack(new StringStream(self::vectorBytes(
            'share-group-describe',
            'sharegroupdescribe.v1.stable.response'
        )));
        $group = $described->groups['t3-41-vectors-group'];
        self::assertSame('simple', $group->assignorName);
        self::assertCount(1, $group->members);
        self::assertSame(['t3-41-vectors' => [0]], array_values($group->members)[0]->assignment->partitionsByTopic());
    }

    /**
     * An acknowledgement batch has one type for the whole range, or one per offset
     */
    public function testAnAcknowledgementBatchCarriesOneTypeOrOnePerOffset(): void
    {
        self::assertSame([0, 1, 2, 3], [
            ShareAcknowledgementBatch::GAP,
            ShareAcknowledgementBatch::ACCEPT,
            ShareAcknowledgementBatch::RELEASE,
            ShareAcknowledgementBatch::REJECT,
        ]);

        $range = ShareAcknowledgementBatch::of(3, 5, ShareAcknowledgementBatch::RELEASE);
        self::assertSame([3, 5, [ShareAcknowledgementBatch::RELEASE]], [$range->firstOffset, $range->lastOffset, $range->acknowledgeTypes]);
        self::assertSame(
            ['firstOffset' => BinarySchema::TYPE_INT64, 'lastOffset' => BinarySchema::TYPE_INT64, 'acknowledgeTypes' => [BinarySchema::TYPE_INT8]],
            ShareAcknowledgementBatch::getScheme()
        );
    }

    /**
     * `topicsOf()` names every fetched partition, and a partition that is only acknowledged as well
     */
    public function testTheTopicsOfAShareFetchJoinThePartitionsAndTheAcknowledgements(): void
    {
        $topicA = str_repeat("\x0a", 16);
        $topicB = str_repeat("\x0b", 16);
        $accept = ShareAcknowledgementBatch::of(0, 4, ShareAcknowledgementBatch::ACCEPT);

        $topics = ShareFetchRequest::topicsOf([$topicA => [0, 1]], [$topicA => [1 => [$accept]], $topicB => [2 => [$accept]]]);

        self::assertEquals(
            [
                new ShareFetchRequestTopic($topicA, [new ShareFetchRequestPartition(0, []), new ShareFetchRequestPartition(1, [$accept])]),
                new ShareFetchRequestTopic($topicB, [new ShareFetchRequestPartition(2, [$accept])]),
            ],
            $topics
        );
        self::assertEquals(
            [new ShareAcknowledgeRequestTopic($topicB, [new ShareAcknowledgeRequestPartition(2, [$accept])])],
            ShareAcknowledgeRequest::topicsOf([$topicB => [2 => [$accept]]])
        );
    }

    /**
     * The fields of a ShareFetch are in the order of the spec: the two limits of version 1 behind `max_bytes`
     */
    public function testAShareFetchCarriesTheLimitsOfVersionOneBehindMaxBytes(): void
    {
        self::assertSame(
            ['groupId', 'memberId', 'shareSessionEpoch', 'maxWaitMs', 'minBytes', 'maxBytes', 'maxRecords', 'batchSize', 'topics', 'forgottenTopicsData'],
            array_slice(array_keys(ShareFetchRequestV1::getScheme()), -10)
        );
        self::assertSame(
            ['throttleTimeMs', 'errorCode', 'errorMessage', 'acquisitionLockTimeoutMs', 'responses', 'nodeEndpoints'],
            array_slice(array_keys(ShareFetchResponse::getScheme()), -6)
        );
        self::assertSame(
            array_keys(ShareFetchResponseV1::getScheme()),
            array_keys(ShareFetchResponse::getScheme()),
            'version 2 added no field to the answer'
        );

        $request = new ShareFetchRequest('g', 'm', ShareFetchRequest::INITIAL_EPOCH);
        self::assertSame(0, $request->getShareSessionEpoch());
        self::assertSame([], $request->getTopics());
        self::assertSame(-1, ShareFetchRequest::FINAL_EPOCH);
    }

    /**
     * Version 2 (Kafka 4.2, KIP-1206 and KIP-1222) puts the acquire mode and the renew flag behind `batch_size`
     */
    public function testAShareFetchOfVersionTwoCarriesTheAcquireModeAndTheRenewFlagBehindTheBatchSize(): void
    {
        self::assertSame(
            ['maxRecords', 'batchSize', 'shareAcquireMode', 'isRenewAck', 'topics', 'forgottenTopicsData'],
            array_slice(array_keys(ShareFetchRequest::getScheme()), -6)
        );
        self::assertSame(BinarySchema::TYPE_INT8, ShareFetchRequest::getScheme()['shareAcquireMode']);
        self::assertSame(BinarySchema::TYPE_BOOLEAN, ShareFetchRequest::getScheme()['isRenewAck']);

        $arguments = ['g', 'm', 1, [], 0, 0, 0, 0, 0, [], 'c', 7];
        $renew     = bin2hex((string) new ShareFetchRequest(...[...$arguments, ShareFetchRequest::SHARE_ACQUIRE_MODE_RECORD_LIMIT, true]));
        $version1  = bin2hex((string) new ShareFetchRequestV1(...[...$arguments, ShareFetchRequest::SHARE_ACQUIRE_MODE_RECORD_LIMIT, true]));

        // Size ApiKey Version Correlation ClientId(int16) tags | group member epoch 5 x int32 mode flag topics forgotten tags
        self::assertSame(
            '0000002d' . '004e' . '0002' . '00000007' . '0001' . '63' . '00'
            . '0267' . '026d' . '00000001' . '00000000' . '00000000' . '00000000' . '00000000' . '00000000'
            . '01' . '01' . '01' . '01' . '00',
            $renew
        );
        self::assertSame(
            '0000002b' . '004e' . '0001' . '00000007' . '0001' . '63' . '00'
            . '0267' . '026d' . '00000001' . '00000000' . '00000000' . '00000000' . '00000000' . '00000000'
            . '01' . '01' . '00',
            $version1,
            'version 1 writes neither the mode nor the flag'
        );
        self::assertSame(1, ShareFetchRequest::SHARE_ACQUIRE_MODE_RECORD_LIMIT);
        self::assertSame(0, ShareFetchRequest::SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED);
    }

    /**
     * Version 2 (Kafka 4.2, KIP-1222): the renew flag behind the epoch, and the lock timeout behind the error message
     */
    public function testAShareAcknowledgeOfVersionTwoCarriesTheRenewFlagAndItsAnswerTheLockTimeout(): void
    {
        self::assertSame(
            ['groupId', 'memberId', 'shareSessionEpoch', 'isRenewAck', 'topics'],
            array_slice(array_keys(ShareAcknowledgeRequest::getScheme()), -5)
        );
        self::assertSame(
            ['groupId', 'memberId', 'shareSessionEpoch', 'topics'],
            array_slice(array_keys(ShareAcknowledgeRequestV1::getScheme()), -4)
        );
        self::assertSame(
            ['throttleTimeMs', 'errorCode', 'errorMessage', 'acquisitionLockTimeoutMs', 'responses', 'nodeEndpoints'],
            array_slice(array_keys(ShareAcknowledgeResponse::getScheme()), -6)
        );
        self::assertSame(
            ['throttleTimeMs', 'errorCode', 'errorMessage', 'responses', 'nodeEndpoints'],
            array_slice(array_keys(ShareAcknowledgeResponseV1::getScheme()), -5)
        );

        $renew = ShareAcknowledgeResponse::unpack(new StringStream(self::vectorBytes(
            'share-acknowledge',
            'shareacknowledge.v2.renew.response'
        )));
        self::assertSame(30000, $renew->acquisitionLockTimeoutMs);
        self::assertSame(4, ShareAcknowledgementBatch::RENEW);
    }

    /**
     * `acquiredRecords()` reads the records of the answer that this member holds, with the count of their range
     */
    public function testTheAcquiredRecordsAreTheRangesOfTheAnswerNotTheWholeBatch(): void
    {
        $answer    = ShareFetchResponse::unpack(new StringStream(self::vectorBytes(
            'share-fetch',
            'sharefetch.v1.acknowledge-and-redeliver.response'
        )));
        $topicId   = $answer->responses[0]->topicId;
        $partition = $answer->partitionOf($topicId, 0);

        self::assertNotNull($partition);
        self::assertNull($answer->partitionOf($topicId, 7));
        self::assertNull($answer->partitionOf(str_repeat("\0", 16), 0));
        self::assertCount(6, $partition->records(), 'the whole batch 0-5 travels');
        self::assertSame(
            [1 => ['v1', 2], 3 => ['v3', 2], 4 => ['v4', 2], 5 => ['v5', 2]],
            array_map(
                static fn(array $acquired): array => [$acquired['record']->value, $acquired['deliveryCount']],
                $partition->acquiredRecords()
            ),
            'only the released records are acquired again, with the delivery count 2'
        );
        self::assertSame([0, 0], [$partition->currentLeader->leaderId, $partition->currentLeader->leaderEpoch]);
    }

    /**
     * A partition that acquired nothing carries the empty (not the null) records
     */
    public function testAPartitionThatAcquiredNothingHasNoRecords(): void
    {
        $answer    = ShareFetchResponse::unpack(new StringStream(self::vectorBytes(
            'share-fetch',
            'sharefetch.v1.acknowledge-error.response'
        )));
        $partition = $answer->responses[0]->partitions[0];

        self::assertSame('', $partition->records);
        self::assertSame([], $partition->records());
        self::assertSame([], $partition->acquiredRecords());
        self::assertSame(121, $partition->acknowledgeErrorCode);
        self::assertSame(0, new ShareLeaderIdAndEpoch()->leaderId, 'the spec gives the leader no default');
    }

    private static function vectorBytes(string $api, string $id): string
    {
        foreach (VectorFile::read($api)['vectors'] as $vector) {
            if ($vector['id'] === $id) {
                return (string) hex2bin((string) $vector['hex']);
            }
        }

        self::fail("no vector {$id} in {$api}.json");
    }
}
