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
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;

/**
 * Byte-exact tests for the OffsetFetch API (key 9), versions 0 and 1.
 *
 * @see docs/protocol/0.10.2.md, section "OffsetFetch API (key 9, v0 and v1)"
 */
#[CoversClass(OffsetFetchRequest::class)]
#[CoversClass(OffsetFetchRequestV0::class)]
#[CoversClass(OffsetFetchResponse::class)]
#[CoversClass(OffsetFetchResponseTopic::class)]
#[CoversClass(OffsetFetchResponsePartition::class)]
#[CoversClass(PartitionsForTopic::class)]
final class OffsetFetchTest extends TestCase
{
    /**
     * OffsetFetch request v1 for the partitions 0 and 1 of "topic", group "my-group".
     *
     *   Size          => 00 00 00 2f (47 bytes)
     *   ApiKey        => 00 09
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   ConsumerGroup => 00 08 "my-group"
     *   [Topic]       => 00 00 00 01
     *     TopicName   => 00 05 "topic"
     *     [Partition] => 00 00 00 02
     *       Partition => 00 00 00 00
     *       Partition => 00 00 00 01
     */
    private const string REQUEST_BODY_HEX = '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000'
        . '00000001';

    private const string REQUEST_V1_HEX = '0000002f' . '0009' . '0001' . self::REQUEST_BODY_HEX;

    /**
     * Version 0 is byte for byte the version 1 request, only the ApiVersion field of the header differs
     */
    private const string REQUEST_V0_HEX = '0000002f' . '0009' . '0000' . self::REQUEST_BODY_HEX;

    /**
     * OffsetFetch response with the committed offset 42 and the metadata "meta" for "topic"-0.
     *
     *   Size            => 00 00 00 27 (39 bytes)
     *   CorrelationId   => 00 00 00 01
     *   [Topic]         => 00 00 00 01
     *     TopicName     => 00 05 "topic"
     *     [Partition]   => 00 00 00 01
     *       Partition   => 00 00 00 00
     *       Offset      => 00 00 00 00 00 00 00 2a
     *       Metadata    => 00 04 "meta"
     *       ErrorCode   => 00 00
     */
    private const string RESPONSE_HEX = '00000027'
        . '00000001'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . '0004' . '6d657461'
        . '0000';

    /**
     * The answer of a v1 request for a topic-partition that has never been committed: offset -1, no metadata and no
     * error. A v0 request answers the same partition with the error code 3, UnknownTopicOrPartition.
     */
    private const string RESPONSE_WITHOUT_OFFSET_HEX = '00000023'
        . '00000001'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . 'ffffffffffffffff'
        . 'ffff'
        . '0000';

    public function testVersion1RequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetFetchRequest('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_FETCH, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
    }

    public function testVersion0RequestOnlyDiffersInTheVersionFieldOfTheHeader(): void
    {
        $request = new OffsetFetchRequestV0('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(
            OffsetFetchRequest::getScheme(),
            OffsetFetchRequestV0::getScheme(),
            'both versions of the request are described by the same scheme'
        );
    }

    public function testTopicArrayIsNotNullableInThisProtocolLine(): void
    {
        $scheme = OffsetFetchRequest::getScheme();

        self::assertSame(['topic' => PartitionsForTopic::class], $scheme['topicPartitions']);
    }

    public function testAlreadyBuiltTopicPartitionsArePackedAsTheyAre(): void
    {
        $request = new OffsetFetchRequest(
            'my-group',
            ['topic' => new PartitionsForTopic('topic', [0, 1])],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
    }

    public function testSeveralTopicsAreRequestedInOneMessage(): void
    {
        $request = new OffsetFetchRequest('my-group', ['first' => [0], 'second' => [1, 2]], '', 0);
        $hex     = bin2hex((string) $request);

        self::assertStringContainsString('00000002' . '0005' . bin2hex('first') . '00000001' . '00000000', $hex);
        self::assertStringContainsString(
            '0006' . bin2hex('second') . '00000002' . '00000001' . '00000002',
            $hex
        );
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = OffsetFetchResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(['topic'], array_keys($response->topics));
        self::assertSame([0], array_keys($response->topics['topic']->partitions));

        $partition = $response->topics['topic']->partitions[0];
        self::assertSame(0, $partition->partition);
        self::assertSame(42, $partition->offset);
        self::assertSame('meta', $partition->metadata);
        self::assertSame(0, $partition->errorCode);
    }

    public function testNullMetadataIsUnpackedAsNullAndNotAsAnEmptyString(): void
    {
        $frame     = (string) hex2bin(self::RESPONSE_WITHOUT_OFFSET_HEX);
        $response  = OffsetFetchResponse::unpack(new StringStream($frame));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(-1, $partition->offset, 'a topic-partition without a committed offset answers with -1');
        self::assertNull($partition->metadata);
        self::assertSame(0, $partition->errorCode);
    }

    public function testUnknownTopicPartitionOfVersion0IsReportedPerPartition(): void
    {
        $frame = '00000023'
            . '00000001'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . 'ffffffffffffffff'
            . 'ffff'
            . '0003';

        $response = OffsetFetchResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(3, $response->topics['topic']->partitions[0]->errorCode);
        self::assertSame(-1, $response->topics['topic']->partitions[0]->offset);
    }

    public function testEmptyMetadataIsUnpackedAsAnEmptyString(): void
    {
        $frame = '00000023'
            . '00000001'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '0000000000000000'
            . '0000'
            . '0000';

        $response = OffsetFetchResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame('', $response->topics['topic']->partitions[0]->metadata);
    }
}
