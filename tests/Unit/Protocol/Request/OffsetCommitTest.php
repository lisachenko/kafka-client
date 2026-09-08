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
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV0;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV1;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponseTopic;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV1;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;

/**
 * Byte-exact tests for the OffsetCommit API (key 8), versions 0, 1 and 2.
 *
 * @see docs/protocol/0.9.0.md, section "OffsetCommit API (key 8, v0, v1 and v2)"
 */
#[CoversClass(OffsetCommitRequest::class)]
#[CoversClass(OffsetCommitRequestV0::class)]
#[CoversClass(OffsetCommitRequestV1::class)]
#[CoversClass(OffsetCommitResponse::class)]
#[CoversClass(OffsetCommitRequestTopic::class)]
#[CoversClass(OffsetCommitRequestTopicV0::class)]
#[CoversClass(OffsetCommitRequestTopicV1::class)]
#[CoversClass(OffsetCommitRequestPartition::class)]
#[CoversClass(OffsetCommitRequestPartitionV0::class)]
#[CoversClass(OffsetCommitRequestPartitionV1::class)]
#[CoversClass(OffsetCommitResponseTopic::class)]
#[CoversClass(OffsetCommitResponsePartition::class)]
final class OffsetCommitTest extends TestCase
{
    /**
     * OffsetCommit request v2, group "my-group", offset 42 of "topic"-0, without metadata.
     *
     *   Size          => 00 00 00 43 (67 bytes)
     *   ApiKey        => 00 08
     *   ApiVersion    => 00 02
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
    private const string REQUEST_V2_HEX = '00000043'
        . '0008'
        . '0002'
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

    public function testVersion2RequestIsPackedAccordingToTheSpec(): void
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

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_COMMIT, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion());
    }

    public function testExplicitRetentionTimeIsPackedInVersion2(): void
    {
        // One hour in milliseconds, 00 00 00 00 00 36 ee 80, instead of the -1 that asks for the broker default
        $request = new OffsetCommitRequest('my-group', -1, '', 3600000, ['topic' => [0 => 42]], 'test', 1);

        self::assertSame(
            str_replace('ffffffffffffffff' . '00000001', '000000000036ee80' . '00000001', self::REQUEST_V2_HEX),
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
        self::assertStringEndsWith('000000000000002a' . '0000', bin2hex((string) $withEmptyString));
        self::assertSame(67, $withEmptyString->getMessageSize(), 'both markers are two bytes wide');
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

        // Version 2 has no field for it: the very same timestamp simply does not reach the wire
        $v2 = new OffsetCommitRequest(
            'my-group',
            -1,
            '',
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            ['topic' => [0 => new OffsetCommitRequestPartition(0, 42, null, 1451606400000)]],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $v2));
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
            [...$header, 'consumerGroup', 'generationId', 'memberName', 'retentionTime', 'topicPartitions'],
            array_keys(OffsetCommitRequest::getScheme())
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
            ['partition', 'offset', 'metadata'],
            array_keys(OffsetCommitRequestPartition::getScheme())
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
        $response = OffsetCommitResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(['topic'], array_keys($response->topics));
        self::assertSame('topic', $response->topics['topic']->topic);
        self::assertSame([0], array_keys($response->topics['topic']->partitions));
        self::assertSame(0, $response->topics['topic']->partitions[0]->partition);
        self::assertSame(0, $response->topics['topic']->partitions[0]->errorCode);
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

        $response = OffsetCommitResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(12, $response->topics['topic']->partitions[0]->errorCode);
    }
}
