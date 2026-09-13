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

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\NewPartitionReassignment;
use Protocol\Kafka\Admin\PartitionReassignment;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ListPartitionReassignmentsTopics;
use Protocol\Kafka\Protocol\Data\OngoingPartitionReassignment;
use Protocol\Kafka\Protocol\Data\OngoingTopicReassignment;
use Protocol\Kafka\Protocol\Data\ReassignablePartition;
use Protocol\Kafka\Protocol\Data\ReassignablePartitionResponse;
use Protocol\Kafka\Protocol\Data\ReassignableTopic;
use Protocol\Kafka\Protocol\Data\ReassignableTopicResponse;
use Protocol\Kafka\Protocol\Request\AlterPartitionReassignmentsRequest;
use Protocol\Kafka\Protocol\Request\AlterPartitionReassignmentsResponse;
use Protocol\Kafka\Protocol\Request\ListPartitionReassignmentsRequest;
use Protocol\Kafka\Protocol\Request\ListPartitionReassignmentsResponse;

/**
 * Byte-exact tests for the two partition-reassignment apis of KIP-455 (keys 45 and 46, Kafka 2.4).
 *
 * Both are **flexible from their version 0** - they were added by the release that introduced KIP-482 - so every
 * frame here is a compact one: the request header v2 with its tag buffer, compact strings and arrays, and a
 * tagged-field section at the end of every structure. That makes them the shortest illustration of what the
 * flexible encoding looks like in an api that carries strings and nested arrays.
 *
 * @see docs/protocol/2.8.md, sections "AlterPartitionReassignments API (key 45, v0)" and
 *      "ListPartitionReassignments API (key 46, v0)"
 */
#[CoversClass(AlterPartitionReassignmentsRequest::class)]
#[CoversClass(AlterPartitionReassignmentsResponse::class)]
#[CoversClass(ListPartitionReassignmentsRequest::class)]
#[CoversClass(ListPartitionReassignmentsResponse::class)]
#[CoversClass(ReassignableTopic::class)]
#[CoversClass(ReassignablePartition::class)]
#[CoversClass(ReassignableTopicResponse::class)]
#[CoversClass(ReassignablePartitionResponse::class)]
#[CoversClass(ListPartitionReassignmentsTopics::class)]
#[CoversClass(OngoingTopicReassignment::class)]
#[CoversClass(OngoingPartitionReassignment::class)]
#[CoversClass(NewPartitionReassignment::class)]
#[CoversClass(PartitionReassignment::class)]
final class PartitionReassignmentTest extends TestCase
{
    /**
     * AlterPartitionReassignments request v0: `events-0` to the brokers 0 and 1, and `events-1` cancelled.
     *
     *   Size           => 00 00 00 32 (50 bytes)
     *   ApiKey         => 00 2d (45), ApiVersion => 00 00
     *   CorrelationId  => 00 00 00 05
     *   ClientId       => 00 04 "test"
     *   TAG_BUFFER     => 00                    (of the request header v2)
     *   TimeoutMs      => 00 00 ea 60 (60000)
     *   Topics         => 02                    (compact: one topic)
     *     Name         => 07 "events"           (compact: 6 + 1)
     *     Partitions   => 03                    (compact: two partitions)
     *       PartitionIndex => 00 00 00 00, Replicas => 03 [0, 1], TAG_BUFFER => 00
     *       PartitionIndex => 00 00 00 01, Replicas => 00 (the compact null: cancel), TAG_BUFFER => 00
     *     TAG_BUFFER   => 00                    (of the topic)
     *   TAG_BUFFER     => 00                    (of the body)
     */
    private const string ALTER_REQUEST_HEX = '00000032'
        . '002d'
        . '0000'
        . '00000005'
        . '0004' . '74657374'
        . '00'
        . '0000ea60'
        . '02'
        . '07' . '6576656e7473'
        . '03'
        . '00000000' . '03' . '00000000' . '00000001' . '00'
        . '00000001' . '00' . '00'
        . '00'
        . '00';

    /**
     * The answer to it: the first partition accepted, the second one refused with the code 85.
     *
     *   Size           => 00 00 00 2b (43 bytes)
     *   CorrelationId  => 00 00 00 05
     *   TAG_BUFFER     => 00                    (of the response header v1)
     *   ThrottleTimeMs => 00 00 00 00
     *   ErrorCode      => 00 00
     *   ErrorMessage   => 01                    (the compact EMPTY string, which is what the broker sends)
     *   Responses      => 02 07 "events" 03
     *     00 00 00 00, ErrorCode 00 00, ErrorMessage 00 (null), TAG_BUFFER 00
     *     00 00 00 01, ErrorCode 00 55 (85), ErrorMessage 05 "late", TAG_BUFFER 00
     */
    private const string ALTER_RESPONSE_HEX = '0000002b'
        . '00000005'
        . '00'
        . '00000000'
        . '0000'
        . '01'
        . '02' . '07' . '6576656e7473' . '03'
        . '00000000' . '0000' . '00' . '00'
        . '00000001' . '0055' . '05' . '6c617465' . '00'
        . '00'
        . '00';

    /**
     * ListPartitionReassignments request v0 for two partitions of one topic.
     */
    private const string LIST_REQUEST_HEX = '00000026'
        . '002e'
        . '0000'
        . '00000006'
        . '0004' . '74657374'
        . '00'
        . '0000ea60'
        . '02'
        . '07' . '6576656e7473'
        . '03' . '00000000' . '00000001'
        . '00'
        . '00';

    /**
     * An answer with one partition in flight: `events-0` lives on 0, 1 and 2 while 2 is being added and 0 removed.
     */
    private const string LIST_RESPONSE_HEX = '00000033'
        . '00000006'
        . '00'
        . '00000000'
        . '0000'
        . '00'
        . '02' . '07' . '6576656e7473' . '02'
        . '00000000'
        . '04' . '00000000' . '00000001' . '00000002'
        . '02' . '00000002'
        . '02' . '00000000'
        . '00'
        . '00'
        . '00';

    public function testTheAlterRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new AlterPartitionReassignmentsRequest(
            ['events' => [0 => new NewPartitionReassignment([0, 1]), 1 => null]],
            60000,
            'test',
            5
        );

        self::assertSame(self::ALTER_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::ALTER_PARTITION_REASSIGNMENTS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(60000, $request->getTimeoutMs());
        self::assertTrue(
            AlterPartitionReassignmentsRequest::isFlexible(),
            'the api was added by Kafka 2.4 and is flexible from its version 0'
        );
        self::assertSame(AlterPartitionReassignmentsRequest::HEADER_V2, $request->getHeaderVersion());
    }

    /**
     * A plain list of broker ids is the same request as a {@see NewPartitionReassignment}
     */
    public function testTheTargetReplicasCanBeGivenAsAPlainList(): void
    {
        $request = new AlterPartitionReassignmentsRequest(['events' => [0 => [0, 1], 1 => null]], 60000, 'test', 5);

        self::assertSame(self::ALTER_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testAnEmptyTargetReplicaSetIsRefusedBeforeItReachesTheBroker(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new NewPartitionReassignment([]);
    }

    public function testTheAlterAnswerCarriesOneErrorPerPartition(): void
    {
        $response = AlterPartitionReassignmentsResponse::unpack(
            new StringStream((string) hex2bin(self::ALTER_RESPONSE_HEX))
        );

        self::assertSame(5, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'the top level says nothing about them');
        self::assertSame('', $response->errorMessage, 'and the broker sends the empty string, not null, for it');
        self::assertSame(['events'], array_keys($response->responses));

        $partitions = $response->responses['events']->partitions;

        self::assertSame([0, 1], array_keys($partitions), 'the answer is indexed by the partition index');
        self::assertSame(KafkaException::NO_ERROR, $partitions[0]->errorCode);
        self::assertNull($partitions[0]->errorMessage, 'a partition without an error carries a null message');
        self::assertSame(KafkaException::NO_REASSIGNMENT_IN_PROGRESS, $partitions[1]->errorCode);
        self::assertSame('late', $partitions[1]->errorMessage);
        self::assertSame(self::ALTER_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testTheListRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new ListPartitionReassignmentsRequest(['events' => [0, 1]], 60000, 'test', 6);

        self::assertSame(self::LIST_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::LIST_PARTITION_REASSIGNMENTS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertTrue(ListPartitionReassignmentsRequest::isFlexible());
    }

    /**
     * The null topic array asks for the whole cluster and the empty one for nothing, and they differ by one byte
     */
    public function testTheNullTopicArrayIsNotTheEmptyOne(): void
    {
        $everything = bin2hex((string) new ListPartitionReassignmentsRequest(null, 60000, 'test', 7));
        $nothing    = bin2hex((string) new ListPartitionReassignmentsRequest([], 60000, 'test', 8));

        self::assertStringEndsWith('0000ea60' . '00' . '00', $everything, 'the compact null array is 00');
        self::assertStringEndsWith('0000ea60' . '01' . '00', $nothing, 'and the empty one is 01');
    }

    public function testTheListAnswerDescribesWhatIsMoving(): void
    {
        $response = ListPartitionReassignmentsResponse::unpack(
            new StringStream((string) hex2bin(self::LIST_RESPONSE_HEX))
        );

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(['events'], array_keys($response->topics));

        $partition = $response->topics['events']->partitions[0];

        self::assertSame([0, 1, 2], $partition->replicas, 'the union of the old and the new replica set');
        self::assertSame([2], $partition->addingReplicas);
        self::assertSame([0], $partition->removingReplicas);
        self::assertSame(self::LIST_RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The value object of the answer does the arithmetic of the three replica lists
     */
    public function testThePartitionReassignmentValueObjectNamesBothEnds(): void
    {
        $reassignment = new PartitionReassignment('events', 0, [0, 1, 2], [2], [0]);

        self::assertSame([1, 2], $reassignment->getTargetReplicas(), 'the set without the ones that are leaving');
        self::assertSame([0, 1], $reassignment->getOriginalReplicas(), 'and without the ones that are arriving');
    }
}
