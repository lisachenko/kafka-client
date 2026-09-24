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
use Protocol\Kafka\Protocol\Data\ClientMetricsResource;
use Protocol\Kafka\Protocol\Data\ClientMetricsResourceV0;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestPartition;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\ShareGroupStateBatch;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestPartition;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesRequest;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesRequestV0;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesResponse;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesResponseV0;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateResponse;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryRequest;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryRequestV0;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryResponse;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryResponseV0;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateRequestV0;

/**
 * Byte-exact tests for what Kafka 4.1 added to T1's surface: the key 74 at version 1 (ListConfigResources,
 * KIP-1142) and the share-group state apis 83 to 87 of KIP-932, which are wire only.
 *
 * The frames are the ones the 4.3.1 node answered; `tests/Compliance` replays them from `docs/protocol/vectors/`,
 * and this class asserts what a caller reads of them.
 *
 * @see docs/protocol/4.3.md, sections "The config resources of KIP-1142 (v1)" and "The share-group state apis (keys
 *      83 to 87) — wire only"
 */
#[CoversClass(ListClientMetricsResourcesRequest::class)]
#[CoversClass(ListClientMetricsResourcesRequestV0::class)]
#[CoversClass(ListClientMetricsResourcesResponse::class)]
#[CoversClass(ListClientMetricsResourcesResponseV0::class)]
#[CoversClass(ClientMetricsResource::class)]
#[CoversClass(ClientMetricsResourceV0::class)]
#[CoversClass(ReadShareGroupStateRequest::class)]
#[CoversClass(ReadShareGroupStateResponse::class)]
#[CoversClass(ReadShareGroupStateSummaryRequest::class)]
#[CoversClass(ReadShareGroupStateSummaryResponse::class)]
#[CoversClass(ReadShareGroupStateSummaryRequestV0::class)]
#[CoversClass(ReadShareGroupStateSummaryResponseV0::class)]
#[CoversClass(WriteShareGroupStateRequest::class)]
#[CoversClass(WriteShareGroupStateRequestV0::class)]
final class ConfigResourcesAndShareStateTest extends TestCase
{
    private const string CLIENT = '0012' . '6b61666b612d636c69656e742d74312d3431';

    /**
     * The topic id of the share-state captures
     */
    private const string TOPIC_ID_HEX = '1d5d10c01ac6490d8b5b1faa8c75c66c';

    private const string GROUP = '1a' . '74312d34312d6e6f2d737563682d73686172652d67726f7570';

    /**
     * The version 1 request carries the types, a compact array of int8, where the version 0 carries nothing
     */
    public function testTheVersionOneRequestCarriesTheResourceTypes(): void
    {
        $request = new ListClientMetricsResourcesRequest(
            'kafka-client-t1-41',
            4511,
            [
                ListClientMetricsResourcesRequest::RESOURCE_TYPE_CLIENT_METRICS,
                ListClientMetricsResourcesRequest::RESOURCE_TYPE_GROUP,
            ]
        );

        self::assertSame(ApiKeys::LIST_CLIENT_METRICS_RESOURCES, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
        self::assertSame([16, 32], $request->getResourceTypes());
        self::assertSame(
            '00000021004a00010000119f' . self::CLIENT . '00' . '03' . '10' . '20' . '00',
            bin2hex((string) $request),
            'the two types behind the compact length 3 (2 + 1)'
        );
        self::assertSame(
            '0000001e004a00000000119f' . self::CLIENT . '00' . '00',
            bin2hex((string) new ListClientMetricsResourcesRequestV0('kafka-client-t1-41', 4511, [16, 32])),
            'the version 0 frame has no field for them'
        );
    }

    /**
     * The version 1 answer is a list of resources with their type; a broker comes back once per type
     */
    public function testTheVersionOneAnswerIsAListOfTypedResources(): void
    {
        $hex      = '00000015' . '000011a0' . '00' . '00000000' . '0000' . '03' . '0231' . '08' . '00' . '0231' . '04'
            . '00' . '00';
        $response = ListClientMetricsResourcesResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertCount(2, $response->clientMetricsResources);
        self::assertSame([0, 1], array_keys($response->clientMetricsResources), 'a list, not keyed by the name');
        self::assertSame('1', $response->clientMetricsResources[0]->name);
        self::assertSame(ListClientMetricsResourcesRequest::RESOURCE_TYPE_BROKER_LOGGER, $response->clientMetricsResources[0]->resourceType);
        self::assertSame('1', $response->clientMetricsResources[1]->name);
        self::assertSame(ListClientMetricsResourcesRequest::RESOURCE_TYPE_BROKER, $response->clientMetricsResources[1]->resourceType);
        self::assertSame($hex, bin2hex((string) $response));

        $v0 = ListClientMetricsResourcesResponseV0::unpack(
            new StringStream((string) hex2bin('0000001c00000e7f000000000000000' . '2' . '0e74312d33372d6d65747269637300' . '00'))
        );

        self::assertInstanceOf(ClientMetricsResourceV0::class, $v0->clientMetricsResources['t1-37-metrics']);
        self::assertSame(16, $v0->clientMetricsResources['t1-37-metrics']->resourceType, 'the default of the field');
    }

    /**
     * A ReadShareGroupState names its topics by id and every partition by its index and leader epoch
     */
    public function testTheShareStateReadIsGroupTopicIdAndPartition(): void
    {
        $request = new ReadShareGroupStateRequest(
            't1-41-no-such-share-group',
            [
                new ReadShareGroupStateRequestTopic(
                    (string) hex2bin(self::TOPIC_ID_HEX),
                    [new ReadShareGroupStateRequestPartition(0, -1)]
                ),
            ],
            'kafka-client-t1-41',
            4702
        );

        self::assertSame(ApiKeys::READ_SHARE_GROUP_STATE, $request->getApiKey());
        self::assertSame('t1-41-no-such-share-group', $request->getGroupId());
        self::assertSame(
            '00000054' . '0054' . '0000' . '0000125e' . self::CLIENT . '00' . self::GROUP
            . '02' . self::TOPIC_ID_HEX . '02' . '00000000' . 'ffffffff' . '00' . '00' . '00',
            bin2hex((string) $request)
        );

        $summary = new ReadShareGroupStateSummaryRequestV0(
            $request->getGroupId(),
            $request->getTopics(),
            'kafka-client-t1-41',
            4706
        );

        self::assertSame(
            str_replace(['00540000', '0000125e'], ['00570000', '00001262'], bin2hex((string) $request)),
            bin2hex((string) $summary),
            'the summary is the frame of the read with another api key'
        );
    }

    /**
     * The read of an uninitialized partition is the 42 per partition, with the zeros of the schema behind it
     */
    public function testTheReadOfAnUninitializedPartitionIsInvalidRequest(): void
    {
        $message = 'Read operation on uninitialized share partition not allowed.';
        $hex     = '0000006a' . '0000125e' . '00' . '02' . self::TOPIC_ID_HEX . '02' . '00000000' . '002a'
            . '3d' . bin2hex($message) . '00000000' . '0000000000000000' . '01' . '00' . '00' . '00';

        $response  = ReadShareGroupStateResponse::unpack(new StringStream((string) hex2bin($hex)));
        $partition = $response->results[0]->partitions[0];

        self::assertSame((string) hex2bin(self::TOPIC_ID_HEX), $response->results[0]->topicId);
        self::assertSame(KafkaException::INVALID_REQUEST, $partition->errorCode);
        self::assertSame($message, $partition->errorMessage);
        self::assertSame(0, $partition->startOffset);
        self::assertSame([], $partition->stateBatches);
        self::assertSame($hex, bin2hex((string) $response));

        $summary = ReadShareGroupStateSummaryResponseV0::unpack(new StringStream((string) hex2bin(
            '00000031' . '00001262' . '00' . '02' . self::TOPIC_ID_HEX . '02' . '00000000' . '0000' . '00'
            . '00000000' . '00000000' . 'ffffffffffffffff' . '00' . '00' . '00'
        )));

        self::assertSame(-1, $summary->results[0]->partitions[0]->startOffset, 'the summary is the initial state');
    }

    /**
     * A write carries the state batches of a partition, the four fields of every batch
     */
    public function testAWriteCarriesTheStateBatches(): void
    {
        $request = new WriteShareGroupStateRequestV0(
            'g',
            [
                new WriteShareGroupStateRequestTopic(
                    (string) hex2bin(self::TOPIC_ID_HEX),
                    [new WriteShareGroupStateRequestPartition(0, 1, 2, 3, [new ShareGroupStateBatch(3, 5, 2, 1)])]
                ),
            ],
            'test',
            9
        );

        self::assertSame(
            '0055' . '0000' . '00000009' . '0004' . '74657374' . '00'
            . '02' . '67'
            . '02' . self::TOPIC_ID_HEX
            . '02' . '00000000' . '00000001' . '00000002' . '0000000000000003'
            . '02' . '0000000000000003' . '0000000000000005' . '02' . '0001' . '00'
            . '00' . '00' . '00',
            substr(bin2hex((string) $request), 8)
        );
    }
}
