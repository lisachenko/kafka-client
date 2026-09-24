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
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestPartition;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponsePartition;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponseTopic;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponseTopicV0;
use Protocol\Kafka\Protocol\Data\ShareGroupStateBatch;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestPartition;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestPartitionV0;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopicV0;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryRequest;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryRequestV0;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryResponse;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryResponseV0;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateRequestV0;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateResponse;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateResponseV0;

/**
 * Byte-exact tests for the `DeliveryCompleteCount` of KIP-1226 (Kafka 4.2): WriteShareGroupState v1 (key 85) and
 * ReadShareGroupStateSummary v1 (key 87), next to the version 0 frames of the same questions
 *
 * The frames are the ones captured on the 4.3.1 node (`writesharegroupstate.*.v1.uninitialized` and
 * `readsharegroupstatesummary.*.v1.delivery-complete`): a write of the partition 0 of a topic of the capture for a
 * share group that does not exist, which the share coordinator refuses with the 42, and the summary of the same
 * partition for a share group that had fetched and acknowledged records - the start offset 0 and the count 2.
 *
 * @see docs/protocol/4.3.md, sections "WriteShareGroupState API (key 85, v0 and v1)" and "ReadShareGroupStateSummary
 *      API (key 87, v0 and v1)"
 */
#[CoversClass(WriteShareGroupStateRequest::class)]
#[CoversClass(WriteShareGroupStateRequestV0::class)]
#[CoversClass(WriteShareGroupStateResponse::class)]
#[CoversClass(WriteShareGroupStateResponseV0::class)]
#[CoversClass(WriteShareGroupStateRequestTopic::class)]
#[CoversClass(WriteShareGroupStateRequestTopicV0::class)]
#[CoversClass(WriteShareGroupStateRequestPartition::class)]
#[CoversClass(WriteShareGroupStateRequestPartitionV0::class)]
#[CoversClass(ReadShareGroupStateSummaryRequest::class)]
#[CoversClass(ReadShareGroupStateSummaryRequestV0::class)]
#[CoversClass(ReadShareGroupStateSummaryResponse::class)]
#[CoversClass(ReadShareGroupStateSummaryResponseV0::class)]
#[CoversClass(ReadShareGroupStateSummaryResponseTopic::class)]
#[CoversClass(ReadShareGroupStateSummaryResponseTopicV0::class)]
#[CoversClass(ReadShareGroupStateSummaryResponsePartition::class)]
#[CoversClass(ReadShareGroupStateSummaryResponsePartitionV0::class)]
final class ShareStateDeliveryCountTest extends TestCase
{
    /**
     * The client id `kafka-client-t1-42` of the capture, int16 length
     */
    private const string CLIENT = '0012' . '6b61666b612d636c69656e742d74312d3432';

    /**
     * The topic id of the capture (the topic was deleted afterwards)
     */
    private const string TOPIC_ID_HEX = '2851a90357c046bfb222a2188345333e';

    /**
     * The share group `t1-42-no-such-share-group`, compact
     */
    private const string GROUP = '1a' . '74312d34322d6e6f2d737563682d73686172652d67726f7570';

    /**
     * The partition 0 of the write: state epoch 0, leader epoch 0, start offset 0
     */
    private const string PARTITION_HEAD = '00000000' . '00000000' . '00000000' . '0000000000000000';

    /**
     * One batch, the offsets 0 and 1 acknowledged (`02`) and delivered once, and the tag buffers of the batch array
     */
    private const string BATCHES = '02' . '0000000000000000' . '0000000000000001' . '02' . '0001' . '00';

    /**
     * Version 1 puts the count between the start offset and the batches; a v0 request of the same topics drops it
     */
    public function testTheWriteOfVersionOneCarriesTheCountBetweenTheStartOffsetAndTheBatches(): void
    {
        $topics = [
            new WriteShareGroupStateRequestTopic(
                (string) hex2bin(self::TOPIC_ID_HEX),
                [new WriteShareGroupStateRequestPartition(0, 0, 0, 0, [new ShareGroupStateBatch(0, 1, 2, 1)], 2)]
            ),
        ];
        $request = new WriteShareGroupStateRequest('t1-42-no-such-share-group', $topics, 'kafka-client-t1-42', 4213);

        self::assertSame(ApiKeys::WRITE_SHARE_GROUP_STATE, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            '00000079' . '0055' . '0001' . '00001075' . self::CLIENT . '00' . self::GROUP
            . '02' . self::TOPIC_ID_HEX . '02' . self::PARTITION_HEAD . '00000002' . self::BATCHES
            . '00' . '00' . '00',
            bin2hex((string) $request),
            'writesharegroupstate.request.v1.uninitialized, as the node received it'
        );

        $v0 = new WriteShareGroupStateRequestV0('t1-42-no-such-share-group', $topics, 'kafka-client-t1-42', 4213);

        self::assertSame(0, $v0->getApiVersion());
        self::assertInstanceOf(WriteShareGroupStateRequestTopicV0::class, $v0->getTopics()[0]);
        self::assertInstanceOf(WriteShareGroupStateRequestPartitionV0::class, $v0->getTopics()[0]->partitions[0]);
        self::assertSame(2, $v0->getTopics()[0]->partitions[0]->deliveryCompleteCount, 'kept, but never written');
        self::assertSame(
            '00000075' . '0055' . '0000' . '00001075' . self::CLIENT . '00' . self::GROUP
            . '02' . self::TOPIC_ID_HEX . '02' . self::PARTITION_HEAD . self::BATCHES
            . '00' . '00' . '00',
            bin2hex((string) $v0)
        );
    }

    /**
     * Each topic class holds the partitions of its own version, whatever it is given
     */
    public function testATopicConvertsItsPartitionsIntoTheEntryOfItsVersion(): void
    {
        $v0Partition = new WriteShareGroupStateRequestPartitionV0(3, 1, 2, 7);
        $v1Topic     = new WriteShareGroupStateRequestTopic('0123456789abcdef', [$v0Partition]);

        self::assertSame(WriteShareGroupStateRequestPartition::class, $v1Topic->partitions[3]::class);
        self::assertSame(-1, $v1Topic->partitions[3]->deliveryCompleteCount, 'the default of the field');
        self::assertSame(7, $v1Topic->partitions[3]->startOffset);

        $v1Partition = new WriteShareGroupStateRequestPartition(3);
        self::assertSame($v1Partition, new WriteShareGroupStateRequestTopic('0123456789abcdef', [$v1Partition])->partitions[3]);
        self::assertSame(-1, $v1Partition->deliveryCompleteCount);
    }

    /**
     * The answer of a write is the frame of version 0 - the 42 of an uninitialized share partition
     */
    public function testTheAnswerOfAWriteIsTheFrameOfVersionZero(): void
    {
        $message = 'Write operation on uninitialized share partition not allowed.';
        $hex     = '0000005e' . '00001075' . '00' . '02' . self::TOPIC_ID_HEX . '02' . '00000000' . '002a'
            . '3e' . bin2hex($message) . '00' . '00' . '00';

        foreach ([WriteShareGroupStateResponse::class, WriteShareGroupStateResponseV0::class] as $class) {
            $response  = $class::unpack(new StringStream((string) hex2bin($hex)));
            $partition = $response->results[0]->partitions[0];

            self::assertSame(KafkaException::INVALID_REQUEST, $partition->errorCode);
            self::assertSame($message, $partition->errorMessage);
            self::assertSame($hex, bin2hex((string) $response));
        }
    }

    /**
     * The summary of version 1 appends the count to every partition; the request is the frame of version 0
     */
    public function testTheSummaryOfVersionOneAppendsTheCountToThePartition(): void
    {
        $topics  = [
            new ReadShareGroupStateRequestTopic(
                (string) hex2bin(self::TOPIC_ID_HEX),
                [new ReadShareGroupStateRequestPartition(0, -1)]
            ),
        ];
        $request = new ReadShareGroupStateSummaryRequest('t1-42-no-such-share-group', $topics, 'kafka-client-t1-42', 4215);
        $v0      = new ReadShareGroupStateSummaryRequestV0('t1-42-no-such-share-group', $topics, 'kafka-client-t1-42', 4215);

        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            '00000054' . '0057' . '0001' . '00001077' . self::CLIENT . '00' . self::GROUP
            . '02' . self::TOPIC_ID_HEX . '02' . '00000000' . 'ffffffff' . '00' . '00' . '00',
            bin2hex((string) $request)
        );
        self::assertSame(
            str_replace('00570001', '00570000', bin2hex((string) $request)),
            bin2hex((string) $v0),
            'the version 1 request is the version 0 frame with another version in the header'
        );

        // readsharegroupstatesummary.response.v1.delivery-complete: start offset 0, count 2
        $hex = '00000035' . '00001074' . '00' . '02' . self::TOPIC_ID_HEX . '02' . '00000000' . '0000' . '00'
            . '00000002' . '00000000' . '0000000000000000' . '00000002' . '00' . '00' . '00';

        $response  = ReadShareGroupStateSummaryResponse::unpack(new StringStream((string) hex2bin($hex)));
        $partition = $response->results[0]->partitions[0];

        self::assertInstanceOf(ReadShareGroupStateSummaryResponseTopic::class, $response->results[0]);
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(2, $partition->stateEpoch);
        self::assertSame(0, $partition->startOffset);
        self::assertSame(2, $partition->deliveryCompleteCount);
        self::assertSame($hex, bin2hex((string) $response));
    }

    /**
     * The summary of version 0 ends every partition with the start offset, and its count stays the -1 of the default
     */
    public function testTheSummaryOfVersionZeroHasNoCount(): void
    {
        $hex = '00000031' . '00001074' . '00' . '02' . self::TOPIC_ID_HEX . '02' . '00000000' . '0000' . '00'
            . '00000002' . '00000000' . '0000000000000000' . '00' . '00' . '00';

        $response  = ReadShareGroupStateSummaryResponseV0::unpack(new StringStream((string) hex2bin($hex)));
        $partition = $response->results[0]->partitions[0];

        self::assertInstanceOf(ReadShareGroupStateSummaryResponseTopicV0::class, $response->results[0]);
        self::assertInstanceOf(ReadShareGroupStateSummaryResponsePartitionV0::class, $partition);
        self::assertSame(0, $partition->startOffset);
        self::assertSame(-1, $partition->deliveryCompleteCount, 'not on the wire: the default of the field');
        self::assertSame($hex, bin2hex((string) $response));
    }
}
