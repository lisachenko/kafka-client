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
use Protocol\Kafka\Admin\ClusterDescription;
use Protocol\Kafka\Admin\ProducerState;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeClusterBroker;
use Protocol\Kafka\Protocol\Data\DescribeProducersRequestTopic;
use Protocol\Kafka\Protocol\Data\DescribeProducersResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeProducersResponseTopic;
use Protocol\Kafka\Protocol\Data\ProducerState as ProducerStateData;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequest;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponse;
use Protocol\Kafka\Protocol\Request\DescribeProducersRequest;
use Protocol\Kafka\Protocol\Request\DescribeProducersResponse;

/**
 * Byte-exact tests for the two apis Kafka 2.8 added to the admin surface: DescribeCluster (key 60, KIP-700) and
 * DescribeProducers (key 61, KIP-664).
 *
 * Both are flexible from their version 0. The first is the **smallest request of this protocol** - a single
 * boolean - and the second is the producer state of a log made visible, which is the only place in this protocol
 * where a producer epoch is an `int32`.
 *
 * @see docs/protocol/2.8.md, sections "DescribeCluster API (key 60, v0)" and "DescribeProducers API (key 61, v0)"
 */
#[CoversClass(DescribeClusterRequest::class)]
#[CoversClass(DescribeClusterResponse::class)]
#[CoversClass(DescribeProducersRequest::class)]
#[CoversClass(DescribeProducersResponse::class)]
#[CoversClass(DescribeClusterBroker::class)]
#[CoversClass(DescribeProducersRequestTopic::class)]
#[CoversClass(DescribeProducersResponseTopic::class)]
#[CoversClass(DescribeProducersResponsePartition::class)]
#[CoversClass(ProducerStateData::class)]
#[CoversClass(ClusterDescription::class)]
#[CoversClass(ProducerState::class)]
final class ClusterAndProducersTest extends TestCase
{
    /**
     * DescribeCluster request v0 without the acl flag - the whole body is one byte.
     *
     *   Size          => 00 00 00 11 (17 bytes)
     *   ApiKey        => 00 3c (60), ApiVersion => 00 00
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test", TAG_BUFFER => 00
     *   IncludeClusterAuthorizedOperations => 00
     *   TAG_BUFFER    => 00
     */
    private const string CLUSTER_REQUEST_HEX = '00000011'
        . '003c'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '00'
        . '00';

    /**
     * An answer with one broker, no rack, and the acl bit field of a request that did not ask for it.
     */
    private const string CLUSTER_RESPONSE_HEX = '00000032'
        . '00000009'
        . '00'
        . '00000000'
        . '0000'
        . '00'
        . '08' . '74312d636c7573'
        . '00000000'
        . '02'
        . '00000000' . '0a' . '3132372e302e302e31' . '00002384' . '00' . '00'
        . '80000000'
        . '00';

    /**
     * DescribeProducers request v0 for one partition of one topic.
     */
    private const string PRODUCERS_REQUEST_HEX = '0000001e'
        . '003d'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '02'
        . '07' . '6576656e7473'
        . '02' . '00000000'
        . '00'
        . '00';

    /**
     * An answer with one producer that has a transaction open at the offset 12.
     */
    private const string PRODUCERS_RESPONSE_HEX = '00000042'
        . '00000009'
        . '00'
        . '00000000'
        . '02'
        . '07' . '6576656e7473'
        . '02'
        . '00000000'
        . '0000'
        . '00'
        . '02'
        . '0000000000000152' . '00000003' . '00000007' . '0000017f00000000' . '00000002'
        . '000000000000000c' . '00'
        . '00'
        . '00'
        . '00';

    public function testTheClusterRequestIsTheSmallestOfThisProtocol(): void
    {
        $request = new DescribeClusterRequest(false, 'test', 9);

        self::assertSame(self::CLUSTER_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_CLUSTER, $request->getApiKey());
        self::assertTrue(DescribeClusterRequest::isFlexible());
        self::assertSame(DescribeClusterRequest::HEADER_V2, $request->getHeaderVersion());
        self::assertFalse($request->includesClusterAuthorizedOperations());

        $asking = new DescribeClusterRequest(true, 'test', 9);

        self::assertTrue($asking->includesClusterAuthorizedOperations());
        self::assertSame(
            1,
            strlen((string) $asking) - strlen((string) $request) + 1,
            'the two frames are the same length and differ in exactly one byte'
        );
        self::assertNotSame(bin2hex((string) $request), bin2hex((string) $asking));
    }

    public function testTheClusterAnswerNamesTheBrokersAndTheController(): void
    {
        $response = DescribeClusterResponse::unpack(new StringStream((string) hex2bin(self::CLUSTER_RESPONSE_HEX)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame('t1-clus', $response->clusterId);
        self::assertSame(0, $response->controllerId);
        self::assertSame([0], array_keys($response->brokers));
        self::assertSame('127.0.0.1', $response->brokers[0]->host);
        self::assertSame(9092, $response->brokers[0]->port);
        self::assertNull($response->brokers[0]->rack, 'a broker without a rack sends the compact null');
        self::assertSame(
            DescribeClusterResponse::OPERATIONS_NOT_REQUESTED,
            $response->clusterAuthorizedOperations,
            'Integer.MIN_VALUE is the default of the specification, not an error'
        );
        self::assertSame(self::CLUSTER_RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The acl bit field of KIP-430 is a set of `AclOperation` bits, and -2147483648 is "not asked"
     */
    public function testTheAuthorizedOperationsAreABitFieldOrTheAbsentDefault(): void
    {
        $notAsked = new ClusterDescription('t1-clus', 0, []);
        $answered = new ClusterDescription('t1-clus', 0, [], 8096);

        self::assertFalse($notAsked->hasAuthorizedOperations());
        self::assertSame(-2147483648, $notAsked->authorizedOperations);
        self::assertTrue($answered->hasAuthorizedOperations());
        self::assertSame(
            [5, 7, 8, 9, 10, 11, 12],
            array_values(array_filter(range(0, 31), static fn(int $bit): bool => (8096 & (1 << $bit)) !== 0)),
            'CREATE, ALTER, DESCRIBE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS and IDEMPOTENT_WRITE'
        );
        self::assertNull($notAsked->controller(), 'a description without brokers has no controller to hand out');
    }

    public function testTheProducersRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DescribeProducersRequest(['events' => [0]], 'test', 9);

        self::assertSame(self::PRODUCERS_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_PRODUCERS, $request->getApiKey());
        self::assertTrue(DescribeProducersRequest::isFlexible());
        self::assertSame(['events'], array_keys($request->getTopics()));
        self::assertSame([0], $request->getTopics()['events']->partitionIndexes);
    }

    public function testTheProducerStateOfAnOpenTransactionIsReadBack(): void
    {
        $response = DescribeProducersResponse::unpack(
            new StringStream((string) hex2bin(self::PRODUCERS_RESPONSE_HEX))
        );

        $partition = $response->topics['events']->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertNull($partition->errorMessage);
        self::assertSame([338], array_keys($partition->activeProducers), 'indexed by the producer id');

        $state = $partition->activeProducers[338];

        self::assertSame(338, $state->producerId);
        self::assertSame(3, $state->producerEpoch, 'an int32 here, an int16 in every other api');
        self::assertSame(7, $state->lastSequence);
        self::assertSame(2, $state->coordinatorEpoch);
        self::assertSame(12, $state->currentTxnStartOffset);
        self::assertSame(self::PRODUCERS_RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The -1 of the specification is `null` in the value object, so that a caller can simply ask
     */
    public function testAnOpenTransactionIsANullableOffsetInTheValueObject(): void
    {
        $open   = new ProducerState(338, 3, 7, 1, 2, 12);
        $closed = new ProducerState(338, 3, 7, 1, -1, null);

        self::assertTrue($open->hasOpenTransaction());
        self::assertSame(12, $open->currentTransactionStartOffset);
        self::assertFalse($closed->hasOpenTransaction());
        self::assertNull($closed->currentTransactionStartOffset);
    }
}
