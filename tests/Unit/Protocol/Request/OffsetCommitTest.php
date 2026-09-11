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
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartitionV0;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartitionV1;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartitionV2;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV0;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV1;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV2;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponseTopic;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV1;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV2;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV3;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV4;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV5;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV1;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV2;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV3;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV4;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV5;

/**
 * Byte-exact tests for the OffsetCommit API (key 8), versions 0 to 6.
 *
 * Version 3 (KIP-124, Kafka 0.11) is the leading `ThrottleTimeMs` of the answer and nothing else: the request of
 * v2 and v3 is one and the same body, and the three lower versions of the answer are one and the same layout.
 * Version 4 (KIP-219, Kafka 2.0) does not touch either half - it is the version from which a throttled broker
 * answers first and mutes the channel afterwards - so the frames of v2, v3 and v4 differ in their api version
 * field alone. Kafka 2.1 changed the frame twice more: version 5 **removes** `retention_time` (KIP-211) and
 * version 6 gives every partition a `committed_leader_epoch` (KIP-320), which is the version this client sends.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v6)"
 */
#[CoversClass(OffsetCommitRequest::class)]
#[CoversClass(OffsetCommitRequestV0::class)]
#[CoversClass(OffsetCommitRequestV1::class)]
#[CoversClass(OffsetCommitRequestV2::class)]
#[CoversClass(OffsetCommitRequestV3::class)]
#[CoversClass(OffsetCommitRequestV5::class)]
#[CoversClass(OffsetCommitRequestV4::class)]
#[CoversClass(OffsetCommitResponse::class)]
#[CoversClass(OffsetCommitResponseV0::class)]
#[CoversClass(OffsetCommitResponseV1::class)]
#[CoversClass(OffsetCommitResponseV2::class)]
#[CoversClass(OffsetCommitResponseV3::class)]
#[CoversClass(OffsetCommitResponseV5::class)]
#[CoversClass(OffsetCommitResponseV4::class)]
#[CoversClass(OffsetCommitRequestTopic::class)]
#[CoversClass(OffsetCommitRequestTopicV0::class)]
#[CoversClass(OffsetCommitRequestTopicV1::class)]
#[CoversClass(OffsetCommitRequestTopicV2::class)]
#[CoversClass(OffsetCommitRequestPartition::class)]
#[CoversClass(OffsetCommitRequestPartitionV0::class)]
#[CoversClass(OffsetCommitRequestPartitionV1::class)]
#[CoversClass(OffsetCommitRequestPartitionV2::class)]
#[CoversClass(OffsetCommitResponseTopic::class)]
#[CoversClass(OffsetCommitResponsePartition::class)]
final class OffsetCommitTest extends TestCase
{
    /**
     * OffsetCommit request v3, group "my-group", offset 42 of "topic"-0, without metadata.
     *
     *   Size          => 00 00 00 43 (67 bytes)
     *   ApiKey        => 00 08
     *   ApiVersion    => 00 03
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   ConsumerGroup => 00 08 "my-group"
     *   GenerationId  => ff ff ff ff (-1, not a member of a group)
     *   MemberName    => 00 00 (empty consumer id, never null)
     *   RetentionTime => ff ff ff ff ff ff ff ff (-1, the retention configured on the broker)
     *   [Topic]       => 00 00 00 01
     *     TopicName   => 00 05 "topic"
     *     [Partition] => 00 00 00 01
     *       Partition => 00 00 00 00
     *       Offset    => 00 00 00 00 00 00 00 2a
     *       Metadata  => ff ff (null)
     *
     * The per-partition TimeStamp of v1 is gone, the request-wide RetentionTime took its place; both are eight
     * bytes wide, so the two frames happen to be the same length.
     */
    private const string REQUEST_V3_HEX = '00000043'
        . '0008'
        . '0003'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . 'ffffffff'
        . '0000'
        . 'ffffffffffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . 'ffff';

    /**
     * The very same commit as {@see self::REQUEST_V3_HEX} sent as version 4 (KIP-219, Kafka 2.0).
     *
     * Only the ApiVersion of the header changes, from 00 03 to 00 04; every other byte is the one of v2 and v3.
     */
    private const string REQUEST_V4_HEX = '00000043'
        . '0008'
        . '0004'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . 'ffffffff'
        . '0000'
        . 'ffffffffffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . 'ffff';

    /**
     * The same commit as version 5 (Kafka 2.1, KIP-211), which has no `retention_time` at all.
     *
     *   Size          => 00 00 00 3b (59 bytes, eight less than v4)
     *   ApiVersion    => 00 05
     *   ConsumerGroup => 00 08 "my-group"
     *   GenerationId  => ff ff ff ff
     *   MemberName    => 00 00
     *   [Topic]       => 00 00 00 01, "topic", one partition, offset 42, metadata null
     */
    private const string REQUEST_V5_HEX = '0000003b'
        . '0008'
        . '0005'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . 'ffffffff'
        . '0000'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . 'ffff';

    /**
     * The same commit as version 6 (Kafka 2.1, KIP-320): a `committed_leader_epoch` behind every offset.
     *
     *   Size               => 00 00 00 3f (63 bytes, four more than v5)
     *   ApiVersion         => 00 06
     *   [Partition]        => 00 00 00 00
     *     Offset           => 00 00 00 00 00 00 00 2a
     *     CommittedLeaderEpoch => ff ff ff ff (-1, the client does not know it)
     *     Metadata         => ff ff
     */
    private const string REQUEST_V6_HEX = '0000003f'
        . '0008'
        . '0006'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . 'ffffffff'
        . '0000'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . 'ffffffff'
        . 'ffff';

    /**
     * OffsetCommit request v1, group "my-group", offset 42 of "topic"-0, without metadata.
     *
     *   Size          => 00 00 00 43 (67 bytes)
     *   ApiKey        => 00 08
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   ConsumerGroup => 00 08 "my-group"
     *   GenerationId  => ff ff ff ff (-1, not a member of a group)
     *   MemberName    => 00 00 (empty consumer id, never null)
     *   [Topic]       => 00 00 00 01
     *     TopicName   => 00 05 "topic"
     *     [Partition] => 00 00 00 01
     *       Partition => 00 00 00 00
     *       Offset    => 00 00 00 00 00 00 00 2a
     *       TimeStamp => ff ff ff ff ff ff ff ff (-1, the broker stamps its receive time)
     *       Metadata  => ff ff (null)
     */
    private const string REQUEST_V1_HEX = '00000043'
        . '0008'
        . '0001'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . 'ffffffff'
        . '0000'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . 'ffffffffffffffff'
        . 'ffff';

    /**
     * The same commit as {@see self::REQUEST_V1_HEX}, with the metadata string "meta" attached to the offset.
     *
     *   Size     => 00 00 00 47 (71 bytes, four characters and their length prefix instead of the null marker)
     *   Metadata => 00 04 "meta"
     */
    private const string REQUEST_V1_WITH_METADATA_HEX = '00000047'
        . '0008'
        . '0001'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . 'ffffffff'
        . '0000'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . 'ffffffffffffffff'
        . '0004' . '6d657461';

    /**
     * OffsetCommit request v0: no generation id, no consumer id and no per-partition timestamp.
     *
     *   Size          => 00 00 00 35 (53 bytes)
     *   ApiKey        => 00 08
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   ConsumerGroup => 00 08 "my-group"
     *   [Topic]       => 00 00 00 01
     *     TopicName   => 00 05 "topic"
     *     [Partition] => 00 00 00 01
     *       Partition => 00 00 00 00
     *       Offset    => 00 00 00 00 00 00 00 2a
     *       Metadata  => ff ff (null)
     */
    private const string REQUEST_V0_HEX = '00000035'
        . '0008'
        . '0000'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . 'ffff';

    /**
     * OffsetCommit response, identical in v0, v1 and v2: one topic with one successful partition.
     *
     *   Size            => 00 00 00 19 (25 bytes)
     *   CorrelationId   => 00 00 00 01
     *   [Topic]         => 00 00 00 01
     *     TopicName     => 00 05 "topic"
     *     [Partition]   => 00 00 00 01
     *       Partition   => 00 00 00 00
     *       ErrorCode   => 00 00
     */
    private const string RESPONSE_HEX = '00000019'
        . '00000001'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '0000';

    public function testVersion6RequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetCommitRequest(
            'my-group',
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => 42]],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V6_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_COMMIT, $request->getApiKey());
        self::assertSame(6, $request->getApiVersion(), 'the highest non-flexible version of the api');
    }

    public function testVersion5DropsTheRetentionTimeFromTheFrame(): void
    {
        $request = new OffsetCommitRequestV5(
            'my-group',
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            3600000,
            ['topic' => [0 => 42]],
            'test',
            1
        );

        self::assertSame(
            self::REQUEST_V5_HEX,
            bin2hex((string) $request),
            'KIP-211 took the field out of the frame: an explicit retention is simply not written by v5'
        );
        self::assertSame(5, $request->getApiVersion());
        self::assertSame(59, $request->getMessageSize(), 'eight bytes shorter than the v4 frame');
    }

    public function testTheLeaderEpochOfAnOffsetIsPackedByVersion6Only(): void
    {
        $withEpoch = new OffsetCommitRequest(
            'my-group',
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => new OffsetAndMetadata(42, null, 7)]],
            'test',
            1
        );

        // The -1 of the "unknown epoch" frame becomes the epoch 7, and nothing else moves
        self::assertSame(
            str_replace('000000000000002a' . 'ffffffff', '000000000000002a' . '00000007', self::REQUEST_V6_HEX),
            bin2hex((string) $withEpoch)
        );

        $version5 = new OffsetCommitRequestV5(
            'my-group',
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => new OffsetAndMetadata(42, null, 7)]],
            'test',
            1
        );

        self::assertSame(
            self::REQUEST_V5_HEX,
            bin2hex((string) $version5),
            'a version below 6 has no field for the epoch, so it never reaches the wire'
        );
    }

    public function testVersion4RequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetCommitRequestV4(
            'my-group',
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => 42]],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V4_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_COMMIT, $request->getApiKey());
        self::assertSame(4, $request->getApiVersion(), 'the last version that carries the retention time');
    }

    public function testVersion3RequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetCommitRequestV3(
            'my-group',
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => 42]],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V3_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_COMMIT, $request->getApiKey());
        self::assertSame(3, $request->getApiVersion());
    }

    public function testTheBodyOfVersionFourIsTheBodyOfVersionThree(): void
    {
        self::assertSame(
            substr(self::REQUEST_V3_HEX, 16),
            substr(self::REQUEST_V4_HEX, 16),
            'KIP-219 raised the api version without adding a field'
        );
    }

    public function testVersion2RequestSendsTheSameBodyAsVersion3(): void
    {
        $request = new OffsetCommitRequestV2(
            'my-group',
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => 42]],
            'test',
            1
        );

        self::assertSame(2, $request->getApiVersion());
        self::assertSame(
            substr(self::REQUEST_V3_HEX, 16),
            substr(bin2hex((string) $request), 16),
            'OFFSET_COMMIT_REQUEST_V3 = OFFSET_COMMIT_REQUEST_V2'
        );
    }

    public function testExplicitRetentionTimeIsPackedInVersion2(): void
    {
        // One hour in milliseconds, 00 00 00 00 00 36 ee 80, instead of the -1 that asks for the broker default
        $request = new OffsetCommitRequestV4('my-group', -1, '', 3600000, ['topic' => [0 => 42]], 'test', 1);

        self::assertSame(
            str_replace('ffffffffffffffff' . '00000001', '000000000036ee80' . '00000001', self::REQUEST_V4_HEX),
            bin2hex((string) $request)
        );
    }

    public function testVersion1RequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetCommitRequestV1(
            'my-group',
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            ['topic' => [0 => 42]],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_COMMIT, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
    }

    public function testMetadataOfAnOffsetIsPackedAsAString(): void
    {
        $request = new OffsetCommitRequestV1(
            'my-group',
            -1,
            '',
            ['topic' => [0 => new OffsetAndMetadata(42, 'meta')]],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V1_WITH_METADATA_HEX, bin2hex((string) $request));
    }

    public function testEmptyMetadataIsNotTheSameAsNoMetadata(): void
    {
        $withEmptyString = new OffsetCommitRequest(
            'my-group',
            -1,
            '',
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => new OffsetAndMetadata(42, '')]],
            'test',
            1
        );

        // The null marker ff ff of the same commit becomes a zero length: an empty string is a value of its own
        self::assertStringEndsWith('000000000000002a' . 'ffffffff' . '0000', bin2hex((string) $withEmptyString));
        self::assertSame(63, $withEmptyString->getMessageSize(), 'both markers are two bytes wide');
    }

    public function testConsumerIdOfAGroupMemberIsPackedAsAString(): void
    {
        $request = new OffsetCommitRequest('my-group', 7, 'consumer-1', -1, ['topic' => [0 => 42]], 'test', 1);

        self::assertStringContainsString(
            '00000007' . '000a' . bin2hex('consumer-1'),
            bin2hex((string) $request)
        );
    }

    public function testExplicitCommitTimestampIsPackedInVersion1Only(): void
    {
        $partition = new OffsetCommitRequestPartitionV1(0, 42, null, 1451606400000);
        $request   = new OffsetCommitRequestV1('my-group', -1, '', ['topic' => [0 => $partition]], 'test', 1);

        // 1451606400000 ms, i.e. 2016-01-01T00:00:00Z, as an INT64
        self::assertStringEndsWith('00000151fa7bdc00' . 'ffff', bin2hex((string) $request));

        // Version 6 has no field for it: the very same timestamp simply does not reach the wire
        $v6 = new OffsetCommitRequest(
            'my-group',
            -1,
            '',
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => new OffsetCommitRequestPartition(0, 42, null, 1451606400000)]],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V6_HEX, bin2hex((string) $v6));
    }

    public function testVersion0RequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetCommitRequestV0('my-group', ['topic' => [0 => 42]], 'test', 1);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_COMMIT, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
    }

    public function testEveryVersionDeclaresExactlyItsOwnFields(): void
    {
        $header = ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId'];

        self::assertSame(
            [...$header, 'consumerGroup', 'generationId', 'memberName', 'topicPartitions'],
            array_keys(OffsetCommitRequest::getScheme()),
            'KIP-211 took the retention time out of the frame at version 5'
        );
        self::assertSame(
            [...$header, 'consumerGroup', 'generationId', 'memberName', 'retentionTime', 'topicPartitions'],
            array_keys(OffsetCommitRequestV4::getScheme())
        );
        self::assertSame(
            [...$header, 'consumerGroup', 'generationId', 'memberName', 'topicPartitions'],
            array_keys(OffsetCommitRequestV1::getScheme())
        );
        self::assertSame(
            [...$header, 'consumerGroup', 'topicPartitions'],
            array_keys(OffsetCommitRequestV0::getScheme())
        );

        self::assertSame(
            ['topic' => OffsetCommitRequestTopic::class],
            OffsetCommitRequest::getScheme()['topicPartitions']
        );
        self::assertSame(
            ['topic' => OffsetCommitRequestTopicV2::class],
            OffsetCommitRequestV4::getScheme()['topicPartitions'],
            'the partition entry of the versions 2 to 5 has no leader epoch'
        );
        self::assertSame(
            ['topic' => OffsetCommitRequestTopicV1::class],
            OffsetCommitRequestV1::getScheme()['topicPartitions']
        );
        self::assertSame(
            ['topic' => OffsetCommitRequestTopicV0::class],
            OffsetCommitRequestV0::getScheme()['topicPartitions']
        );
    }

    public function testOnlyVersion1PacksAPerPartitionTimestamp(): void
    {
        // The layout of a v2 partition entry is the layout of a v0 one again, v1 is the odd one out
        self::assertSame(
            ['partition', 'offset', 'leaderEpoch', 'metadata'],
            array_keys(OffsetCommitRequestPartition::getScheme()),
            'version 6 inserted the leader epoch between the offset and the metadata'
        );
        self::assertSame(
            ['partition', 'offset', 'metadata'],
            array_keys(OffsetCommitRequestPartitionV2::getScheme())
        );
        self::assertSame(
            ['partition', 'offset', 'timestamp', 'metadata'],
            array_keys(OffsetCommitRequestPartitionV1::getScheme())
        );
        self::assertSame(
            ['partition', 'offset', 'metadata'],
            array_keys(OffsetCommitRequestPartitionV0::getScheme())
        );
        self::assertSame(
            BinarySchema::TYPE_NULLABLE_STRING,
            OffsetCommitRequestPartition::getScheme()['metadata']
        );
    }

    public function testSeveralTopicsAndPartitionsAreCommittedInOneRequest(): void
    {
        $request = new OffsetCommitRequest(
            'my-group',
            -1,
            '',
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['first' => [0 => 1, 1 => 2], 'second' => [3 => 4]],
            '',
            0
        );

        $hex = bin2hex((string) $request);

        self::assertStringContainsString('00000002' . '0005' . bin2hex('first') . '00000002', $hex);
        self::assertStringContainsString('0006' . bin2hex('second') . '00000001' . '00000003', $hex);
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = OffsetCommitResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(['topic'], array_keys($response->topics));
        self::assertSame('topic', $response->topics['topic']->topic);
        self::assertSame([0], array_keys($response->topics['topic']->partitions));
        self::assertSame(0, $response->topics['topic']->partitions[0]->partition);
        self::assertSame(0, $response->topics['topic']->partitions[0]->errorCode);
        self::assertSame(0, $response->throttleTimeMs, 'the field arrived with the version 3');
    }

    public function testTheThreeLowerVersionsOfTheAnswerAreOneAndTheSameLayout(): void
    {
        $frame = (string) hex2bin(self::RESPONSE_HEX);

        foreach ([OffsetCommitResponseV0::class, OffsetCommitResponseV1::class, OffsetCommitResponseV2::class] as $class) {
            $response = $class::unpack(new StringStream($frame));

            self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
            self::assertSame([0], array_keys($response->topics['topic']->partitions));
        }
    }

    public function testTheVersion3AndVersion4AnswersStartWithTheThrottleTime(): void
    {
        $frame = '0000001d'
            . '00000001'
            . '00000000'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '0000';

        foreach ([OffsetCommitResponseV3::class, OffsetCommitResponse::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin($frame)));

            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(['topic'], array_keys($response->topics));
            self::assertSame(0, $response->topics['topic']->partitions[0]->errorCode);
            self::assertSame($frame, bin2hex((string) $response));
        }
    }

    public function testPartitionErrorCodeIsReadAsASignedInteger(): void
    {
        $frame = '00000019'
            . '00000001'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '000c'; // 12, OffsetMetadataTooLarge

        $response = OffsetCommitResponseV2::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(12, $response->topics['topic']->partitions[0]->errorCode);
    }
}
