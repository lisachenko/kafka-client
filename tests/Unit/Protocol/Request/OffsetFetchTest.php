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
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopicV0;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV1;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV2;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV3;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV4;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV5;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV6;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV1;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV2;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV3;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV4;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV5;

/**
 * Byte-exact tests for the OffsetFetch API (key 9), versions 0 to 5.
 *
 * The request is one and the same body from version 2 on: version 3 (KIP-124, Kafka 0.11) added the throttle time
 * to the answer and version 4 (KIP-219, Kafka 2.0) added nothing at all, so the five frames differ in their api
 * version field alone, up to and including version 4. Version 5 (Kafka 2.1, KIP-320) is the first one that changes
 * the ANSWER again: every partition entry of it carries a `committed_leader_epoch` behind the committed offset.
 * The request of v5 is still the body of v2, and v5 is the version this client sends.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
#[CoversClass(OffsetFetchRequest::class)]
#[CoversClass(OffsetFetchRequestV0::class)]
#[CoversClass(OffsetFetchRequestV1::class)]
#[CoversClass(OffsetFetchRequestV2::class)]
#[CoversClass(OffsetFetchRequestV3::class)]
#[CoversClass(OffsetFetchRequestV4::class)]
#[CoversClass(OffsetFetchRequestV5::class)]
#[CoversClass(OffsetFetchResponse::class)]
#[CoversClass(OffsetFetchResponseV0::class)]
#[CoversClass(OffsetFetchResponseV1::class)]
#[CoversClass(OffsetFetchResponseV2::class)]
#[CoversClass(OffsetFetchResponseV3::class)]
#[CoversClass(OffsetFetchResponseV4::class)]
#[CoversClass(OffsetFetchResponseV5::class)]
#[CoversClass(OffsetFetchResponseTopic::class)]
#[CoversClass(OffsetFetchResponsePartition::class)]
#[CoversClass(OffsetFetchResponsePartitionV0::class)]
#[CoversClass(OffsetFetchResponseTopicV0::class)]
#[CoversClass(PartitionsForTopic::class)]
final class OffsetFetchTest extends TestCase
{
    /**
     * OffsetFetch request for the partitions 0 and 1 of "topic", group "my-group".
     *
     *   Size          => 00 00 00 2f (47 bytes)
     *   ApiKey        => 00 09
     *   ApiVersion    => 00 0n
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   ConsumerGroup => 00 08 "my-group"
     *   [Topic]       => 00 00 00 01
     *     TopicName   => 00 05 "topic"
     *     [Partition] => 00 00 00 02
     *       Partition => 00 00 00 00
     *       Partition => 00 00 00 01
     *
     * The five versions of the request are byte for byte the same as long as the topics are named explicitly;
     * only the ApiVersion field of the header differs.
     */
    private const string REQUEST_BODY_HEX = '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000'
        . '00000001';

    private const string REQUEST_V5_HEX = '0000002f' . '0009' . '0005' . self::REQUEST_BODY_HEX;

    private const string REQUEST_V4_HEX = '0000002f' . '0009' . '0004' . self::REQUEST_BODY_HEX;

    private const string REQUEST_V3_HEX = '0000002f' . '0009' . '0003' . self::REQUEST_BODY_HEX;

    private const string REQUEST_V2_HEX = '0000002f' . '0009' . '0002' . self::REQUEST_BODY_HEX;

    private const string REQUEST_V1_HEX = '0000002f' . '0009' . '0001' . self::REQUEST_BODY_HEX;

    private const string REQUEST_V0_HEX = '0000002f' . '0009' . '0000' . self::REQUEST_BODY_HEX;

    /**
     * OffsetFetch request v5 that asks for every topic of the group: the topic array is the null one, ff ff ff ff.
     *
     *   Size          => 00 00 00 1c (28 bytes)
     *   ApiKey        => 00 09
     *   ApiVersion    => 00 05
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   ConsumerGroup => 00 08 "my-group"
     *   [Topic]       => ff ff ff ff (null)
     */
    private const string REQUEST_V5_ALL_TOPICS_HEX = '0000001c'
        . '0009' . '0005'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . 'ffffffff';

    /**
     * The same request with an EMPTY topic array, which names no topic at all and is not the same request
     */
    private const string REQUEST_V5_NO_TOPICS_HEX = '0000001c'
        . '0009' . '0005'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000000';

    /**
     * OffsetFetch response v2 with the committed offset 42 and the metadata "meta" for "topic"-0.
     *
     *   Size            => 00 00 00 29 (41 bytes)
     *   CorrelationId   => 00 00 00 01
     *   [Topic]         => 00 00 00 01
     *     TopicName     => 00 05 "topic"
     *     [Partition]   => 00 00 00 01
     *       Partition   => 00 00 00 00
     *       Offset      => 00 00 00 00 00 00 00 2a
     *       Metadata    => 00 04 "meta"
     *       ErrorCode   => 00 00
     *   ErrorCode       => 00 00 (the group-level code of version 2)
     */
    private const string RESPONSE_V2_HEX = '00000029'
        . '00000001'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . '000000000000002a'
        . '0004' . '6d657461'
        . '0000'
        . '0000';

    /**
     * The same answer without the trailing group-level error code, which is what versions 0 and 1 send
     */
    private const string RESPONSE_V1_HEX = '00000027'
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
    private const string RESPONSE_V1_WITHOUT_OFFSET_HEX = '00000023'
        . '00000001'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000'
        . 'ffffffffffffffff'
        . 'ffff'
        . '0000';

    /**
     * A version 2 answer that reports an error of the group itself: no topic at all and the code 16 after them
     *
     *   Size          => 00 00 00 0a (10 bytes)
     *   CorrelationId => 00 00 00 01
     *   [Topic]       => 00 00 00 00
     *   ErrorCode     => 00 10 (16, NotCoordinatorForGroup)
     */
    private const string RESPONSE_V2_GROUP_ERROR_HEX = '0000000a'
        . '00000001'
        . '00000000'
        . '0010';

    public function testVersion5RequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetFetchRequestV5('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(self::REQUEST_V5_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::OFFSET_FETCH, $request->getApiKey());
        self::assertSame(5, $request->getApiVersion(), 'the highest non-flexible version of the api');
        self::assertFalse(OffsetFetchRequestV5::isFlexible());
    }

    /**
     * Version 6 (Kafka 2.4, KIP-482) is the same request in the flexible encoding, and it is what this client sends
     */
    public function testVersion6IsTheFlexibleEncodingOfTheSameRequest(): void
    {
        $request = new OffsetFetchRequestV6('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(
            '0000002a' . '0009' . '0006' . '00000001'
            . '0004' . '74657374'
            . '00'
            . '09' . '6d792d67726f7570'
            . '02'
            . '06' . '746f706963'
            . '03' . '00000000' . '00000001'
            . '00'
            . '00',
            bin2hex((string) $request)
        );
        self::assertSame(6, $request->getApiVersion(), 'KIP-482 made the version 6 the first flexible one');
        self::assertTrue(OffsetFetchRequestV6::isFlexible());
    }

    /**
     * KIP-447 (Kafka 2.5): the boolean `require_stable` is the one field version 7 appended to that frame
     */
    public function testVersion7AppendsTheRequireStableFlagBehindTheTopicArray(): void
    {
        $request = new OffsetFetchRequest('my-group', ['topic' => [0, 1]], 'test', 1, true);

        self::assertSame(
            '0000002b' . '0009' . '0007' . '00000001'
            . '0004' . '74657374'
            . '00'
            . '09' . '6d792d67726f7570'
            . '02'
            . '06' . '746f706963'
            . '03' . '00000000' . '00000001'
            . '00'
            . '01'
            . '00',
            bin2hex((string) $request),
            'the flag is a plain byte between the tag buffer of the topic entry and the one of the body'
        );
        self::assertSame(7, $request->getApiVersion(), 'KIP-447 makes the version this client sends 7');
    }

    /**
     * The flag defaults to false, which is the behaviour - and the frame, one byte longer - of every version below
     */
    public function testTheRequireStableFlagIsFalseByDefault(): void
    {
        $request = new OffsetFetchRequest('my-group', ['topic' => [0, 1]], 'test', 1);
        $below   = new OffsetFetchRequestV6('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertStringEndsWith('00' . '00' . '00', bin2hex((string) $request));
        self::assertSame(
            strlen((string) $below) + 1,
            strlen((string) $request),
            'the version 7 frame is the version 6 frame plus the one byte of the flag'
        );
    }

    /**
     * The nullable topic array of version 2 stays nullable in the flexible encoding: `00` instead of a count
     */
    public function testTheNullTopicArrayOfTheFlexibleVersionIsTheUnsignedVarintZero(): void
    {
        $request = OffsetFetchRequestV6::forAllTopics('my-group', 'test', 1);

        self::assertSame(
            '0000001a' . '0009' . '0006' . '00000001'
            . '0004' . '74657374'
            . '00'
            . '09' . '6d792d67726f7570'
            . '00'
            . '00',
            bin2hex((string) $request),
            'a null compact array is the unsigned varint 0, where an empty one is 1'
        );
    }

    /**
     * The same request of version 7, whose `require_stable` stands behind the null array
     */
    public function testEveryTopicOfAGroupCanBeAskedForWithStableOffsetsOnly(): void
    {
        $request = OffsetFetchRequest::forAllTopics('my-group', 'test', 1, true);

        self::assertSame(
            '0000001b' . '0009' . '0007' . '00000001'
            . '0004' . '74657374'
            . '00'
            . '09' . '6d792d67726f7570'
            . '00'
            . '01'
            . '00',
            bin2hex((string) $request)
        );
    }

    public function testVersion4RequestOnlyDiffersInTheVersionFieldOfTheHeader(): void
    {
        $request = new OffsetFetchRequestV4('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(self::REQUEST_V4_HEX, bin2hex((string) $request));
        self::assertSame(4, $request->getApiVersion());
        self::assertSame(
            OffsetFetchRequestV5::getScheme(),
            OffsetFetchRequestV4::getScheme(),
            'KIP-320 changed the answer of version 5, not its request'
        );
    }

    public function testVersion3RequestOnlyDiffersInTheVersionFieldOfTheHeader(): void
    {
        $request = new OffsetFetchRequestV3('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(self::REQUEST_V3_HEX, bin2hex((string) $request));
        self::assertSame(3, $request->getApiVersion());
        self::assertSame(
            OffsetFetchRequestV5::getScheme(),
            OffsetFetchRequestV3::getScheme(),
            'KIP-219 raised the api version of the request without adding a field'
        );
    }

    public function testVersion2RequestOnlyDiffersInTheVersionFieldOfTheHeader(): void
    {
        $request = new OffsetFetchRequestV2('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion());
        self::assertSame(
            OffsetFetchRequestV5::getScheme(),
            OffsetFetchRequestV2::getScheme(),
            'OFFSET_FETCH_REQUEST_V3 = OFFSET_FETCH_REQUEST_V2'
        );
    }

    public function testVersion1RequestOnlyDiffersInTheVersionFieldOfTheHeader(): void
    {
        $request = new OffsetFetchRequestV1('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
    }

    public function testVersion0RequestOnlyDiffersInTheVersionFieldOfTheHeader(): void
    {
        $request = new OffsetFetchRequestV0('my-group', ['topic' => [0, 1]], 'test', 1);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(
            OffsetFetchRequestV1::getScheme(),
            OffsetFetchRequestV0::getScheme(),
            'the versions 0 and 1 of the request are described by the same scheme'
        );
    }

    public function testTopicArrayIsNullableFromVersionTwoOn(): void
    {
        self::assertSame(
            ['topic' => PartitionsForTopic::class, BinarySchema::FLAG_NULLABLE => true],
            OffsetFetchRequest::getScheme()['topicPartitions']
        );
        self::assertSame(
            ['topic' => PartitionsForTopic::class],
            OffsetFetchRequestV1::getScheme()['topicPartitions'],
            'the nullable topic array arrived with the version 2 in Kafka 0.10.2'
        );
        self::assertSame(
            ['topic' => PartitionsForTopic::class],
            OffsetFetchRequestV0::getScheme()['topicPartitions']
        );
    }

    public function testNullTopicsAreWrittenAsTheNullArrayOfVersionTwo(): void
    {
        $request = new OffsetFetchRequestV5('my-group', null, 'test', 1);

        self::assertSame(self::REQUEST_V5_ALL_TOPICS_HEX, bin2hex((string) $request));
    }

    public function testForAllTopicsBuildsTheNullTopicArrayRequest(): void
    {
        $request = OffsetFetchRequestV5::forAllTopics('my-group', 'test', 1);

        self::assertSame(self::REQUEST_V5_ALL_TOPICS_HEX, bin2hex((string) $request));
    }

    public function testAnEmptyTopicArrayIsADifferentRequestFromTheNullOne(): void
    {
        $request = new OffsetFetchRequestV5('my-group', [], 'test', 1);

        self::assertSame(self::REQUEST_V5_NO_TOPICS_HEX, bin2hex((string) $request));
        self::assertNotSame(self::REQUEST_V5_ALL_TOPICS_HEX, bin2hex((string) $request));
    }

    public function testVersionOneCanNotAskForEveryTopicOfTheGroup(): void
    {
        $this->expectException(UnsupportedVersionException::class);

        new OffsetFetchRequestV1('my-group', null, 'test', 1);
    }

    public function testVersionZeroCanNotAskForEveryTopicOfTheGroup(): void
    {
        $this->expectException(UnsupportedVersionException::class);

        new OffsetFetchRequestV0('my-group', null, 'test', 1);
    }

    public function testAlreadyBuiltTopicPartitionsArePackedAsTheyAre(): void
    {
        $request = new OffsetFetchRequestV5(
            'my-group',
            ['topic' => new PartitionsForTopic('topic', [0, 1])],
            'test',
            1
        );

        self::assertSame(self::REQUEST_V5_HEX, bin2hex((string) $request));
    }

    public function testSeveralTopicsAreRequestedInOneMessage(): void
    {
        $request = new OffsetFetchRequestV5('my-group', ['first' => [0], 'second' => [1, 2]], '', 0);
        $hex     = bin2hex((string) $request);

        self::assertStringContainsString('00000002' . '0005' . bin2hex('first') . '00000001' . '00000000', $hex);
        self::assertStringContainsString(
            '0006' . bin2hex('second') . '00000002' . '00000001' . '00000002',
            $hex
        );
    }

    public function testVersion2ResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = OffsetFetchResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(['topic'], array_keys($response->topics));
        self::assertSame([0], array_keys($response->topics['topic']->partitions));
        self::assertSame(0, $response->errorCode, 'the group-level error code of the version 2');

        $partition = $response->topics['topic']->partitions[0];
        self::assertSame(0, $partition->partition);
        self::assertSame(42, $partition->offset);
        self::assertSame('meta', $partition->metadata);
        self::assertSame(0, $partition->errorCode);
        self::assertSame(0, $response->throttleTimeMs, 'the throttle time arrived with the version 3');
    }

    public function testVersion3AndVersion4PutTheThrottleTimeInFrontOfTheTopicsAndKeepTheGroupErrorLast(): void
    {
        $frame = '0000002d'
            . '00000001'
            . '00000000'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '000000000000002a'
            . '0004' . '6d657461'
            . '0000'
            . '0000';

        foreach ([OffsetFetchResponseV3::class, OffsetFetchResponseV4::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin($frame)));

            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(['topic'], array_keys($response->topics));
            self::assertSame(42, $response->topics['topic']->partitions[0]->offset);
            self::assertSame(0, $response->errorCode, 'the group-level code still closes the answer');
            self::assertSame($frame, bin2hex((string) $response), 'KIP-219 did not touch the answer either');
            self::assertSame(
                OffsetFetchResponsePartitionV0::UNKNOWN_LEADER_EPOCH,
                $response->topics['topic']->partitions[0]->leaderEpoch,
                'an answer below version 5 has no epoch on the wire and leaves the field at -1'
            );
        }
    }

    public function testTheVersion5AnswerCarriesTheLeaderEpochOfEveryCommittedOffset(): void
    {
        // The same answer as the version 4 one, with the four bytes of the epoch behind the committed offset
        $frame = '00000031'
            . '00000001'
            . '00000000'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '000000000000002a'
            . '00000007'
            . '0004' . '6d657461'
            . '0000'
            . '0000';

        $response = OffsetFetchResponseV5::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(42, $response->topics['topic']->partitions[0]->offset);
        self::assertSame(7, $response->topics['topic']->partitions[0]->leaderEpoch);
        self::assertSame('meta', $response->topics['topic']->partitions[0]->metadata);
        self::assertSame(0, $response->topics['topic']->partitions[0]->errorCode);
        self::assertSame($frame, bin2hex((string) $response));

        $committed = $response->topics['topic']->partitions[0]->toOffsetAndMetadata();

        self::assertSame(42, $committed->offset);
        self::assertSame('meta', $committed->metadata);
        self::assertSame(7, $committed->leaderEpoch);
    }

    public function testAnOffsetCommittedWithoutAnEpochIsUnpackedAsANullLeaderEpoch(): void
    {
        $frame = '00000031'
            . '00000001'
            . '00000000'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '000000000000002a'
            . 'ffffffff'
            . '0004' . '6d657461'
            . '0000'
            . '0000';

        $response = OffsetFetchResponseV5::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(-1, $response->topics['topic']->partitions[0]->leaderEpoch);
        self::assertNull(
            $response->topics['topic']->partitions[0]->toOffsetAndMetadata()->leaderEpoch,
            'the -1 of the wire is the empty Optional of the Java OffsetAndMetadata.leaderEpoch()'
        );
    }

    public function testOnlyVersionFiveDeclaresTheLeaderEpochOfAPartition(): void
    {
        self::assertSame(
            ['partition', 'offset', 'leaderEpoch', 'metadata', 'errorCode'],
            array_keys(OffsetFetchResponsePartition::getScheme())
        );
        self::assertSame(
            ['partition', 'offset', 'metadata', 'errorCode'],
            array_keys(OffsetFetchResponsePartitionV0::getScheme())
        );
        self::assertSame(
            ['partition' => OffsetFetchResponsePartition::class],
            OffsetFetchResponseTopic::getScheme()['partitions']
        );
        self::assertSame(
            ['partition' => OffsetFetchResponsePartitionV0::class],
            OffsetFetchResponseTopicV0::getScheme()['partitions']
        );
    }

    public function testAGroupLevelErrorOfVersionTwoComesWithoutAnyTopic(): void
    {
        $frame    = (string) hex2bin(self::RESPONSE_V2_GROUP_ERROR_HEX);
        $response = OffsetFetchResponseV2::unpack(new StringStream($frame));

        self::assertSame(16, $response->errorCode);
        self::assertSame([], $response->topics);
    }

    public function testVersion1ResponseHasNoGroupLevelErrorCode(): void
    {
        $response = OffsetFetchResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertSame(42, $response->topics['topic']->partitions[0]->offset);
        self::assertSame(
            0,
            $response->errorCode,
            'a version below 2 reports every error per partition, so the group code stays 0'
        );
        self::assertSame(
            OffsetFetchResponseV1::getScheme(),
            OffsetFetchResponseV0::getScheme(),
            'the versions 0 and 1 of the answer are described by the same scheme'
        );
    }

    public function testNullMetadataIsUnpackedAsNullAndNotAsAnEmptyString(): void
    {
        $frame     = (string) hex2bin(self::RESPONSE_V1_WITHOUT_OFFSET_HEX);
        $response  = OffsetFetchResponseV1::unpack(new StringStream($frame));
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

        $response = OffsetFetchResponseV0::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(3, $response->topics['topic']->partitions[0]->errorCode);
        self::assertSame(-1, $response->topics['topic']->partitions[0]->offset);
    }

    public function testEmptyMetadataIsUnpackedAsAnEmptyString(): void
    {
        $frame = '00000025'
            . '00000001'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '0000000000000000'
            . '0000'
            . '0000'
            . '0000';

        $response = OffsetFetchResponseV2::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame('', $response->topics['topic']->partitions[0]->metadata);
    }
}
