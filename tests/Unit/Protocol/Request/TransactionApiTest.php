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
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Consumer\ConsumerGroupMetadata;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResponsePartition;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResponseTopic;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResult;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnTransaction;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestPartition;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitResponsePartition;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitResponseTopic;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersRequestMarker;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersRequestMarkerV1;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersResponseMarker;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersResponsePartition;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersResponseTopic;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequestV0;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequestV1;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequestV3;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponse;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponseV0;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponseV1;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponseV2;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponseV3;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequestV0;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequestV1;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequestV3;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequestV4;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponse;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponseV0;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponseV1;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponseV2;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponseV3;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponseV4;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;
use Protocol\Kafka\Protocol\Request\EndTxnRequestV0;
use Protocol\Kafka\Protocol\Request\EndTxnRequestV1;
use Protocol\Kafka\Protocol\Request\EndTxnRequestV3;
use Protocol\Kafka\Protocol\Request\EndTxnRequestV4;
use Protocol\Kafka\Protocol\Request\EndTxnResponse;
use Protocol\Kafka\Protocol\Request\EndTxnResponseV0;
use Protocol\Kafka\Protocol\Request\EndTxnResponseV1;
use Protocol\Kafka\Protocol\Request\EndTxnResponseV2;
use Protocol\Kafka\Protocol\Request\EndTxnResponseV3;
use Protocol\Kafka\Protocol\Request\EndTxnResponseV4;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV1;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV2;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV3;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV4;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV0;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV1;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV2;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV3;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV4;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersRequest;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersRequestV0;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersRequestV1;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersResponse;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersResponseV0;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersResponseV1;

/**
 * Byte-exact tests for the five transaction APIs of Kafka 0.11 (api keys 24 to 28, v0 each).
 *
 * @see docs/protocol/4.3.md, sections "AddPartitionsToTxn API (key 24, v0 to v5)", "AddOffsetsToTxn API (key 25, v0 to v4)",
 *      "EndTxn API (key 26, v0 to v5)", "WriteTxnMarkers API (key 27, v0 to v2)" and "TxnOffsetCommit API (key 28, v0 to v5)"
 */
#[CoversClass(AddPartitionsToTxnRequest::class)]
#[CoversClass(AddPartitionsToTxnRequestV4::class)]
#[CoversClass(AddPartitionsToTxnRequestV3::class)]
#[CoversClass(AddPartitionsToTxnRequestV1::class)]
#[CoversClass(AddPartitionsToTxnRequestV0::class)]
#[CoversClass(AddPartitionsToTxnResponse::class)]
#[CoversClass(AddPartitionsToTxnResponseV4::class)]
#[CoversClass(AddPartitionsToTxnResponseV3::class)]
#[CoversClass(AddPartitionsToTxnResponseV2::class)]
#[CoversClass(AddPartitionsToTxnTransaction::class)]
#[CoversClass(AddPartitionsToTxnResult::class)]
#[CoversClass(AddPartitionsToTxnResponseV1::class)]
#[CoversClass(AddPartitionsToTxnResponseV0::class)]
#[CoversClass(AddPartitionsToTxnResponseTopic::class)]
#[CoversClass(AddPartitionsToTxnResponsePartition::class)]
#[CoversClass(AddOffsetsToTxnRequest::class)]
#[CoversClass(AddOffsetsToTxnRequestV3::class)]
#[CoversClass(AddOffsetsToTxnRequestV1::class)]
#[CoversClass(AddOffsetsToTxnRequestV0::class)]
#[CoversClass(AddOffsetsToTxnResponse::class)]
#[CoversClass(AddOffsetsToTxnResponseV3::class)]
#[CoversClass(AddOffsetsToTxnResponseV2::class)]
#[CoversClass(AddOffsetsToTxnResponseV1::class)]
#[CoversClass(AddOffsetsToTxnResponseV0::class)]
#[CoversClass(EndTxnRequest::class)]
#[CoversClass(EndTxnRequestV4::class)]
#[CoversClass(EndTxnRequestV3::class)]
#[CoversClass(EndTxnRequestV1::class)]
#[CoversClass(EndTxnRequestV0::class)]
#[CoversClass(EndTxnResponse::class)]
#[CoversClass(EndTxnResponseV4::class)]
#[CoversClass(EndTxnResponseV3::class)]
#[CoversClass(EndTxnResponseV2::class)]
#[CoversClass(EndTxnResponseV1::class)]
#[CoversClass(EndTxnResponseV0::class)]
#[CoversClass(WriteTxnMarkersRequest::class)]
#[CoversClass(WriteTxnMarkersRequestV0::class)]
#[CoversClass(WriteTxnMarkersResponse::class)]
#[CoversClass(WriteTxnMarkersResponseV0::class)]
#[CoversClass(WriteTxnMarkersRequestV1::class)]
#[CoversClass(WriteTxnMarkersResponseV1::class)]
#[CoversClass(WriteTxnMarkersRequestMarkerV1::class)]
#[CoversClass(WriteTxnMarkersRequestMarker::class)]
#[CoversClass(WriteTxnMarkersResponseMarker::class)]
#[CoversClass(WriteTxnMarkersResponseTopic::class)]
#[CoversClass(WriteTxnMarkersResponsePartition::class)]
#[CoversClass(TxnOffsetCommitRequest::class)]
#[CoversClass(TxnOffsetCommitRequestV0::class)]
#[CoversClass(TxnOffsetCommitRequestV1::class)]
#[CoversClass(TxnOffsetCommitResponse::class)]
#[CoversClass(TxnOffsetCommitResponseV0::class)]
#[CoversClass(TxnOffsetCommitResponseV1::class)]
#[CoversClass(TxnOffsetCommitRequestTopic::class)]
#[CoversClass(TxnOffsetCommitRequestPartition::class)]
#[CoversClass(TxnOffsetCommitResponseTopic::class)]
#[CoversClass(TxnOffsetCommitResponsePartition::class)]
#[CoversClass(TxnOffsetCommitRequestV4::class)]
#[CoversClass(TxnOffsetCommitRequestV3::class)]
#[CoversClass(TxnOffsetCommitRequestV2::class)]
#[CoversClass(TxnOffsetCommitResponseV4::class)]
#[CoversClass(TxnOffsetCommitResponseV3::class)]
#[CoversClass(TxnOffsetCommitResponseV2::class)]
final class TransactionApiTest extends TestCase
{
    /**
     * AddPartitionsToTxn request v1 for two partitions of one topic.
     *
     *   Size            => 00 00 00 35 (53 bytes)
     *   ApiKey          => 00 18 (24)
     *   ApiVersion      => 00 01
     *   CorrelationId   => 00 00 00 07
     *   ClientId        => 00 04 "test"
     *   TransactionalId => 00 04 "tx-1"
     *   ProducerId      => 00 00 00 00 00 00 00 2a (42)
     *   ProducerEpoch   => 00 03
     *   Topics          => 00 00 00 01
     *     Topic      => 00 05 "topic"
     *     Partitions => 00 00 00 02, 00 00 00 00, 00 00 00 01
     */
    private const string ADD_PARTITIONS_REQUEST_HEX = '00000035'
        . '0018'
        . '0001'
        . '00000007'
        . '0004' . '74657374'
        . '0004' . '74782d31'
        . '000000000000002a'
        . '0003'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002' . '00000000' . '00000001';

    /**
     * AddPartitionsToTxn request **v4** (Kafka 3.5, KIP-890) for the same two partitions, with `verify_only`.
     *
     *   Size            => 00 00 00 33 (51 bytes)
     *   ApiKey          => 00 18 (24)
     *   ApiVersion      => 00 04
     *   CorrelationId   => 00 00 00 07
     *   ClientId        => 00 04 "test"       -- the client id keeps its int16 length in the header v2
     *   TAG_BUFFER      => 00
     *   Transactions    => 02                 -- one entry, compact
     *     TransactionalId => 05 "tx-1"
     *     ProducerId      => 00 00 00 00 00 00 00 2a (42)
     *     ProducerEpoch   => 00 03
     *     VerifyOnly      => 01               -- the field of KIP-890
     *     Topics          => 02
     *       Name       => 06 "topic"
     *       Partitions => 03, 00 00 00 00, 00 00 00 01
     *       TAG_BUFFER => 00
     *     TAG_BUFFER      => 00
     *   TAG_BUFFER      => 00
     */
    private const string ADD_PARTITIONS_V4_REQUEST_HEX = '00000033'
        . '0018'
        . '0004'
        . '00000007'
        . '0004' . '74657374'
        . '00'
        . '02'
        . '05' . '74782d31'
        . '000000000000002a'
        . '0003'
        . '01'
        . '02'
        . '06' . '746f706963'
        . '03' . '00000000' . '00000001'
        . '00'
        . '00'
        . '00';

    /**
     * The first 27 bytes of a version 4 batch of two transactions: the header, the count 2 and the id of the first
     */
    private const string ADD_PARTITIONS_V4_BATCH_PREFIX_HEX = '00000051' . '0018'
        . '0004'
        . '00000007'
        . '0004' . '74657374'
        . '00'
        . '03'
        . '05' . '74782d31';

    /**
     * AddPartitionsToTxn answer **v4**: the top-level code 0, one transaction, the partition 1 not in it (120).
     */
    private const string ADD_PARTITIONS_V4_RESPONSE_HEX = '0000002a'
        . '00000007'
        . '00'
        . '00000000'
        . '0000'
        . '02'
        . '05' . '74782d31'
        . '02'
        . '06' . '746f706963'
        . '03'
        . '00000000' . '0000' . '00'
        . '00000001' . '0078' . '00'
        . '00'
        . '00'
        . '00';

    /**
     * AddPartitionsToTxn answer **v4** of a principal without `CLUSTER_ACTION`: the 31 and no transaction at all.
     */
    private const string ADD_PARTITIONS_V4_REFUSAL_HEX = '0000000d'
        . '00000007'
        . '00'
        . '00000000'
        . '001f'
        . '01'
        . '00';

    /**
     * AddPartitionsToTxn answer **v3**: the flexible frame of Kafka 2.8, which has no top-level error code.
     */
    private const string ADD_PARTITIONS_V3_RESPONSE_HEX = '0000001a'
        . '00000007'
        . '00'
        . '00000000'
        . '02'
        . '06' . '746f706963'
        . '02'
        . '00000000' . '0000' . '00'
        . '00'
        . '00';

    /**
     * AddPartitionsToTxn response v0: the first partition joined the transaction, the second is fenced (47).
     */
    private const string ADD_PARTITIONS_RESPONSE_HEX = '00000023'
        . '00000007'
        . '00000000'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000'
        . '00000001' . '002f';

    /**
     * AddOffsetsToTxn request v0.
     *
     *   Size => 00 00 00 28 (40), ApiKey => 00 19 (25), TransactionalId "tx-1", ProducerId 42, Epoch 3,
     *   GroupId "my-group"
     */
    private const string ADD_OFFSETS_REQUEST_HEX = '00000028'
        . '0019'
        . '0001'
        . '00000008'
        . '0004' . '74657374'
        . '0004' . '74782d31'
        . '000000000000002a'
        . '0003'
        . '0008' . '6d792d67726f7570';

    /**
     * AddOffsetsToTxn response v0 with the error code 48 (InvalidTxnState) and a throttle time of 100 ms
     */
    private const string ADD_OFFSETS_RESPONSE_HEX = '0000000a' . '00000008' . '00000064' . '0030';

    /**
     * EndTxn request v0 that commits: the `TransactionResult` byte is 01
     */
    private const string END_TXN_COMMIT_HEX = '0000001f'
        . '001a'
        . '0001'
        . '00000009'
        . '0004' . '74657374'
        . '0004' . '74782d31'
        . '000000000000002a'
        . '0003'
        . '01';

    /**
     * The same request that aborts: the very same bytes with a 00 as the last one
     */
    private const string END_TXN_ABORT_HEX = '0000001f'
        . '001a'
        . '0001'
        . '00000009'
        . '0004' . '74657374'
        . '0004' . '74782d31'
        . '000000000000002a'
        . '0003'
        . '00';

    /**
     * EndTxn response v0 with the error code 48 (InvalidTxnState)
     */
    private const string END_TXN_RESPONSE_HEX = '0000000a' . '00000009' . '00000000' . '0030';

    /**
     * WriteTxnMarkers request v0 with one marker for one partition.
     *
     *   Size => 00 00 00 34 (52), ApiKey => 00 1b (27), Markers => 1
     *     ProducerId 42, ProducerEpoch 3, TransactionResult 01, Topics => 1 ("topic", partition 0),
     *     CoordinatorEpoch 7
     */
    private const string WRITE_MARKERS_REQUEST_HEX = '00000034'
        . '001b'
        . '0000'
        . '0000000a'
        . '0004' . '74657374'
        . '00000001'
        . '000000000000002a'
        . '0003'
        . '01'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001' . '00000000'
        . '00000007';

    /**
     * WriteTxnMarkers response v0: **no throttle time at all**, the only answer of 0.11 without one
     */
    private const string WRITE_MARKERS_RESPONSE_HEX = '00000025'
        . '0000000a'
        . '00000001'
        . '000000000000002a'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001' . '00000000' . '0034';

    /**
     * TxnOffsetCommit request v0 for one partition with metadata.
     *
     *   Size => 00 00 00 4e (78), ApiKey => 00 1c (28), ApiVersion => 00 02, TransactionalId "tx-1",
     *   GroupId "my-group", ProducerId 42, Epoch 3, Topics => 1 ("topic": partition 0, offset 17,
     *   committed_leader_epoch -1, metadata "state")
     */
    private const string TXN_OFFSET_COMMIT_REQUEST_HEX = '0000004e'
        . '001c'
        . '0002'
        . '0000000b'
        . '0004' . '74657374'
        . '0004' . '74782d31'
        . '0008' . '6d792d67726f7570'
        . '000000000000002a'
        . '0003'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . '0000000000000011' . 'ffffffff' . '0005' . '7374617465';

    /**
     * The same commit as the versions 0 and 1 of Kafka 0.11 and 2.0 send it: four bytes shorter, because the
     * partition entry of those versions has no `committed_leader_epoch` between the offset and the metadata.
     */
    private const string TXN_OFFSET_COMMIT_REQUEST_V0_HEX = '0000004a'
        . '001c'
        . '0000'
        . '0000000b'
        . '0004' . '74657374'
        . '0004' . '74782d31'
        . '0008' . '6d792d67726f7570'
        . '000000000000002a'
        . '0003'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . '0000000000000011' . '0005' . '7374617465';

    /**
     * TxnOffsetCommit response v0: the offset was written, and a throttle time of 5 ms
     */
    private const string TXN_OFFSET_COMMIT_RESPONSE_HEX = '0000001d'
        . '0000000b'
        . '00000005'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . '0000';

    public function testAddPartitionsToTxnRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new AddPartitionsToTxnRequestV1('tx-1', 42, 3, ['topic' => [0, 1]], 'test', 7);

        self::assertSame(self::ADD_PARTITIONS_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::ADD_PARTITIONS_TO_TXN, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised the three apis to version 1');
    }

    public function testAnAlreadyBuiltTopicOfAddPartitionsToTxnIsTakenAsItIs(): void
    {
        $request = new AddPartitionsToTxnRequestV1(
            'tx-1',
            42,
            3,
            ['topic' => new PartitionsForTopic('topic', [0, 1])],
            'test',
            7
        );

        self::assertSame(self::ADD_PARTITIONS_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testAddPartitionsToTxnReportsEveryErrorPerPartitionAndHasNoTopLevelOne(): void
    {
        $response = AddPartitionsToTxnResponseV2::unpack(
            new StringStream((string) hex2bin(self::ADD_PARTITIONS_RESPONSE_HEX))
        );

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['topic'], array_keys($response->errors), 'topics are keyed by their name');

        $partitions = $response->errors['topic']->partitionErrors;
        self::assertSame([0, 1], array_keys($partitions), 'partitions are keyed by their id');
        self::assertSame(KafkaException::NO_ERROR, $partitions[0]->errorCode);
        // 47 is a statement about the transactional id, and it still arrives on every partition of the answer
        self::assertSame(KafkaException::INVALID_PRODUCER_EPOCH, $partitions[1]->errorCode);
        self::assertSame(self::ADD_PARTITIONS_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testAddPartitionsToTxnV4PutsTheTransactionIntoTheBatchArray(): void
    {
        $request = new AddPartitionsToTxnRequestV4('tx-1', 42, 3, ['topic' => [0, 1]], 'test', 7, true);

        self::assertSame(4, $request->getApiVersion(), 'Kafka 3.5 added the version 4 of KIP-890');
        self::assertSame(self::ADD_PARTITIONS_V4_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testAddPartitionsToTxnV4BatchesSeveralTransactionsInOneFrame(): void
    {
        $request = AddPartitionsToTxnRequestV4::forTransactions(
            [
                new AddPartitionsToTxnTransaction('tx-1', 42, 3, ['topic' => [0, 1]], true),
                new AddPartitionsToTxnTransaction('tx-2', 43, 1, ['topic' => new PartitionsForTopic('topic', [0])]),
            ],
            'test',
            7
        );

        $frame = bin2hex((string) $request);
        self::assertStringStartsWith(
            self::ADD_PARTITIONS_V4_BATCH_PREFIX_HEX,
            $frame,
            'the batch is the same body with two entries, so it starts with the count 3 and the first transaction'
        );
        self::assertStringEndsWith(
            '05' . '74782d32' . '000000000000002b' . '0001' . '00'
            . '02' . '06' . '746f706963' . '02' . '00000000' . '00' . '00' . '00',
            $frame,
            'and ends with the second transaction, its own verify_only false and its own tag buffers'
        );
    }

    public function testAnEmptyAddPartitionsToTxnBatchIsRefused(): void
    {
        // A 3.9.2 node answers a `transactions = []` frame with nothing at all and strands the connection
        $this->expectException(InvalidRequestException::class);

        AddPartitionsToTxnRequest::forTransactions([]);
    }

    public function testAVersionBelowFourCanNotCarryMoreThanOneTransaction(): void
    {
        $this->expectException(UnsupportedVersionException::class);

        AddPartitionsToTxnRequestV3::forTransactions(
            [
                new AddPartitionsToTxnTransaction('tx-1', 42, 3, ['topic' => [0]]),
                new AddPartitionsToTxnTransaction('tx-2', 43, 1, ['topic' => [0]]),
            ]
        );
    }

    public function testTheVersion4AnswerCarriesATopLevelErrorCodeAndOneResultPerTransaction(): void
    {
        $response = AddPartitionsToTxnResponse::unpack(
            new StringStream((string) hex2bin(self::ADD_PARTITIONS_V4_RESPONSE_HEX))
        );

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(['tx-1'], array_keys($response->resultsByTransaction));

        $result = $response->resultOf('tx-1');
        self::assertSame('tx-1', $result->transactionalId);
        $partitions = $result->topicResults['topic']->partitionErrors;
        self::assertSame(KafkaException::NO_ERROR, $partitions[0]->errorCode);
        // the code of a partition that is not part of the transaction, which a broker maps to the 48 for a client
        self::assertSame(120, $partitions[1]->errorCode);
        self::assertSame(self::ADD_PARTITIONS_V4_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testTheRefusalOfAWholeVersion4RequestIsTheTopLevelCode(): void
    {
        $response = AddPartitionsToTxnResponse::unpack(
            new StringStream((string) hex2bin(self::ADD_PARTITIONS_V4_REFUSAL_HEX))
        );

        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $response->errorCode);
        self::assertSame([], $response->resultsByTransaction, 'a refused request answers no transaction at all');
        self::assertSame(self::ADD_PARTITIONS_V4_REFUSAL_HEX, bin2hex((string) $response));
    }

    public function testAnAnswerBelowVersion4IsReadAsOneTransactionAsWell(): void
    {
        $response = AddPartitionsToTxnResponseV3::unpack(
            new StringStream((string) hex2bin(self::ADD_PARTITIONS_V3_RESPONSE_HEX))
        );

        $result = $response->resultOf('tx-1');
        self::assertSame('tx-1', $result->transactionalId, 'the id comes from the caller, the answer has none');
        self::assertSame(
            KafkaException::NO_ERROR,
            $result->topicResults['topic']->partitionErrors[0]->errorCode
        );
    }

    public function testAddOffsetsToTxnRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new AddOffsetsToTxnRequestV1('tx-1', 42, 3, 'my-group', 'test', 8);

        self::assertSame(self::ADD_OFFSETS_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::ADD_OFFSETS_TO_TXN, $request->getApiKey());
    }

    public function testAddOffsetsToTxnHasOneErrorCodeForTheWholeRequest(): void
    {
        $response = AddOffsetsToTxnResponseV2::unpack(
            new StringStream((string) hex2bin(self::ADD_OFFSETS_RESPONSE_HEX))
        );

        self::assertSame(100, $response->throttleTimeMs);
        self::assertSame(KafkaException::INVALID_TXN_STATE, $response->errorCode);
        self::assertSame(self::ADD_OFFSETS_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testTheTransactionResultIsTheOnlyDifferenceBetweenACommitAndAnAbort(): void
    {
        $commit = new EndTxnRequestV1('tx-1', 42, 3, EndTxnRequest::COMMIT, 'test', 9);
        $abort  = new EndTxnRequestV1('tx-1', 42, 3, EndTxnRequest::ABORT, 'test', 9);

        self::assertSame(self::END_TXN_COMMIT_HEX, bin2hex((string) $commit));
        self::assertSame(self::END_TXN_ABORT_HEX, bin2hex((string) $abort));
        self::assertTrue(EndTxnRequest::COMMIT);
        self::assertFalse(EndTxnRequest::ABORT);
        self::assertSame(ApiKeys::END_TXN, $commit->getApiKey());
        self::assertSame(
            substr(self::END_TXN_COMMIT_HEX, 0, -2),
            substr(self::END_TXN_ABORT_HEX, 0, -2),
            'the two frames differ in exactly one byte'
        );
    }

    public function testEndTxnResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = EndTxnResponseV2::unpack(new StringStream((string) hex2bin(self::END_TXN_RESPONSE_HEX)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(KafkaException::INVALID_TXN_STATE, $response->errorCode);
        self::assertSame(self::END_TXN_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testWriteTxnMarkersRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new WriteTxnMarkersRequestV0(
            [new WriteTxnMarkersRequestMarker(42, 3, EndTxnRequest::COMMIT, ['topic' => [0]], 7)],
            'test',
            10
        );

        self::assertSame(self::WRITE_MARKERS_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::WRITE_TXN_MARKERS, $request->getApiKey());
    }

    /**
     * The version 2 of Kafka 4.2 (KIP-1228) appends the transaction version to every marker, the version 1 drops it
     */
    public function testTheTransactionVersionOfAMarkerIsTheLastByteOfItInVersionTwo(): void
    {
        $marker = new WriteTxnMarkersRequestMarker(42, 3, EndTxnRequest::ABORT, ['t' => [0]], 7, 2);
        $body   = '02'
            . '000000000000002a' . '0003' . '00'
            . '02' . '0274' . '02' . '00000000' . '00'
            . '00000007';
        $header = '001b' . '%s' . '0000000a' . '0004' . '74657374' . '00';

        $v2 = new WriteTxnMarkersRequest([$marker], 'test', 10);
        $v1 = new WriteTxnMarkersRequestV1([$marker], 'test', 10);

        self::assertSame(2, WriteTxnMarkersRequest::VERSION);
        self::assertSame(2, WriteTxnMarkersResponse::VERSION);
        self::assertSame(self::sized(sprintf($header, '0002') . $body . '02' . '00' . '00'), bin2hex((string) $v2));
        self::assertSame(self::sized(sprintf($header, '0001') . $body . '00' . '00'), bin2hex((string) $v1));
        self::assertSame(0, new WriteTxnMarkersRequestMarker(42, 3)->transactionVersion, 'the default 0 of the field');
        self::assertArrayNotHasKey('transactionVersion', WriteTxnMarkersRequestMarkerV1::getScheme());
        self::assertSame(
            ['producerId', 'producerEpoch', 'transactionResult', 'topics', 'coordinatorEpoch', 'transactionVersion'],
            array_keys(WriteTxnMarkersRequestMarker::getScheme())
        );
    }

    /**
     * A marker given to a request is encoded as the entry of the version of that request
     */
    public function testTheMarkersOfARequestAreTheEntriesOfItsVersion(): void
    {
        $marker = new WriteTxnMarkersRequestMarker(42, 3, EndTxnRequest::COMMIT, ['t' => [0, 1]], 7, 2);

        $v1 = new WriteTxnMarkersRequestV1([$marker]);
        $v0 = new WriteTxnMarkersRequestV0([$marker]);
        $v2 = new WriteTxnMarkersRequest([new WriteTxnMarkersRequestMarkerV1(42, 3)]);

        $entryOf = static fn(WriteTxnMarkersRequest $request): WriteTxnMarkersRequestMarker => (fn(): array => $this->transactionMarkers)->call($request)[42];
        self::assertInstanceOf(WriteTxnMarkersRequestMarkerV1::class, $entryOf($v1));
        self::assertInstanceOf(WriteTxnMarkersRequestMarkerV1::class, $entryOf($v0));
        self::assertSame(WriteTxnMarkersRequestMarker::class, $entryOf($v2)::class);
        self::assertSame([0, 1], $entryOf($v1)->topics['t']->partitions);
        self::assertSame(7, $entryOf($v1)->coordinatorEpoch);
        self::assertSame(2, $entryOf($v1)->transactionVersion, 'kept on the object, only left off the wire');
        self::assertSame(
            ['producerId' => WriteTxnMarkersRequestMarkerV1::class],
            WriteTxnMarkersRequestV1::getScheme()['transactionMarkers']
        );
        self::assertSame(
            WriteTxnMarkersResponseV1::getScheme(),
            WriteTxnMarkersResponse::getScheme(),
            'the version 2 of the answer changed no field'
        );
    }

    public function testTheWriteTxnMarkersAnswerIsTheOnlyOneOf011WithoutAThrottleTime(): void
    {
        $response = WriteTxnMarkersResponseV0::unpack(
            new StringStream((string) hex2bin(self::WRITE_MARKERS_RESPONSE_HEX))
        );

        // `WRITE_TXN_MARKERS_RESPONSE_V0` starts with the array: the api is broker-to-broker and is not throttled
        self::assertArrayNotHasKey('throttleTimeMs', WriteTxnMarkersResponseV0::getScheme());
        self::assertSame([42], array_keys($response->transactionMarkers), 'markers are keyed by the producer id');

        $marker = $response->transactionMarkers[42];
        self::assertSame(42, $marker->producerId);
        self::assertSame(
            KafkaException::TRANSACTION_COORDINATOR_FENCED,
            $marker->topics['topic']->partitions[0]->errorCode
        );
        self::assertSame(self::WRITE_MARKERS_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testTxnOffsetCommitRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new TxnOffsetCommitRequestV2(
            'tx-1',
            'my-group',
            42,
            3,
            ['topic' => [0 => new OffsetAndMetadata(17, 'state')]],
            'test',
            11
        );

        self::assertSame(self::TXN_OFFSET_COMMIT_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::TXN_OFFSET_COMMIT, $request->getApiKey());
    }

    public function testAPlainOffsetOfATxnOffsetCommitCarriesNoMetadata(): void
    {
        $request = new TxnOffsetCommitRequestV2('tx-1', 'my-group', 42, 3, ['topic' => [0 => 17]], 'test', 11);

        // The metadata is a nullable string, so an offset without one is the two bytes ff ff, behind the -1 of
        // the `committed_leader_epoch` that version 2 added
        self::assertStringEndsWith(
            '00000000' . '0000000000000011' . 'ffffffff' . 'ffff',
            bin2hex((string) $request)
        );
    }

    public function testTxnOffsetCommitReportsEveryErrorPerPartition(): void
    {
        $response = TxnOffsetCommitResponseV2::unpack(
            new StringStream((string) hex2bin(self::TXN_OFFSET_COMMIT_RESPONSE_HEX))
        );

        self::assertSame(5, $response->throttleTimeMs);
        self::assertSame(['topic'], array_keys($response->topics));
        self::assertSame(
            KafkaException::NO_ERROR,
            $response->topics['topic']->partitions[0]->errorCode
        );
        self::assertSame(self::TXN_OFFSET_COMMIT_RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The version 0 frames of Kafka 0.11, which Kafka 2.0 raised to version 1 without changing a byte
     * (`ADD_PARTITIONS_TO_TXN_REQUEST_V1 = ADD_PARTITIONS_TO_TXN_REQUEST_V0` and its siblings @ 2.0.1)
     */
    public function testTheVersionZeroFramesAreTheSameBodiesWithALowerVersionField(): void
    {
        $addPartitions = new AddPartitionsToTxnRequestV0('tx-1', 42, 3, ['topic' => [0, 1]], 'test', 7);
        $addOffsets    = new AddOffsetsToTxnRequestV0('tx-1', 42, 3, 'my-group', 'test', 8);
        $endTxn        = new EndTxnRequestV0('tx-1', 42, 3, EndTxnRequest::COMMIT, 'test', 9);
        $txnOffsets    = new TxnOffsetCommitRequestV0(
            'tx-1',
            'my-group',
            42,
            3,
            ['topic' => [0 => new OffsetAndMetadata(17, 'state')]],
            'test',
            11
        );

        self::assertSame(
            substr_replace(self::ADD_PARTITIONS_REQUEST_HEX, '0000', 12, 4),
            bin2hex((string) $addPartitions)
        );
        self::assertSame(substr_replace(self::ADD_OFFSETS_REQUEST_HEX, '0000', 12, 4), bin2hex((string) $addOffsets));
        self::assertSame(substr_replace(self::END_TXN_COMMIT_HEX, '0000', 12, 4), bin2hex((string) $endTxn));
        self::assertSame(self::TXN_OFFSET_COMMIT_REQUEST_V0_HEX, bin2hex((string) $txnOffsets));
        self::assertSame([0, 0, 0, 0], [
            $addPartitions->getApiVersion(),
            $addOffsets->getApiVersion(),
            $endTxn->getApiVersion(),
            $txnOffsets->getApiVersion(),
        ]);
    }

    /**
     * Kafka 2.1 put the `committed_leader_epoch` of KIP-320 into the partition entry of TxnOffsetCommit **v2**
     * alone; the version 1 of Kafka 2.0 is still the body of version 0 with a higher version field
     */
    public function testTheVersionOneOfTxnOffsetCommitIsTheBodyOfVersionZero(): void
    {
        $request = new TxnOffsetCommitRequestV1(
            'tx-1',
            'my-group',
            42,
            3,
            ['topic' => [0 => new OffsetAndMetadata(17, 'state')]],
            'test',
            11
        );

        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr_replace(self::TXN_OFFSET_COMMIT_REQUEST_V0_HEX, '0001', 12, 4),
            bin2hex((string) $request),
            'the entries of version 1 carry no leader epoch either'
        );

        $response = TxnOffsetCommitResponseV1::unpack(
            new StringStream((string) hex2bin(self::TXN_OFFSET_COMMIT_RESPONSE_HEX))
        );

        self::assertSame(self::TXN_OFFSET_COMMIT_RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The leader epoch of version 2 is the one the caller put on the offset, and -1 when there is none
     */
    public function testTheCommittedLeaderEpochOfVersionTwoComesFromTheOffsetValueObject(): void
    {
        $request = new TxnOffsetCommitRequestV2(
            'tx-1',
            'my-group',
            42,
            3,
            ['topic' => [0 => new OffsetAndMetadata(17, 'state', 7)]],
            'test',
            11
        );

        self::assertSame(2, $request->getApiVersion());
        self::assertStringEndsWith(
            '0000000000000011' . '00000007' . '0005' . '7374617465',
            bin2hex((string) $request),
            'the epoch stands between the offset and the metadata'
        );
        $withoutEpoch = new TxnOffsetCommitRequestV2(
            'tx-1',
            'my-group',
            42,
            3,
            ['topic' => [0 => new OffsetAndMetadata(17, 'state')]],
            'test',
            11
        );

        self::assertNull(
            new OffsetAndMetadata(17)->leaderEpoch,
            'an offset whose epoch the client does not know carries null'
        );
        self::assertStringEndsWith(
            '0000000000000011' . 'ffffffff' . '0005' . '7374617465',
            bin2hex((string) $withoutEpoch),
            'and the request writes the -1 of the protocol for it'
        );
        self::assertSame(-1, OffsetAndMetadata::UNKNOWN_LEADER_EPOCH);
    }

    /**
     * KIP-447, Kafka 2.5: the version 3 names the consumer of the group, and it is the first flexible one
     */
    public function testTheVersionThreeOfTxnOffsetCommitCarriesTheConsumerGroupMetadata(): void
    {
        $request = new TxnOffsetCommitRequestV3(
            'tx-1',
            'my-group',
            42,
            3,
            ['topic' => [0 => new OffsetAndMetadata(17, 'state')]],
            new ConsumerGroupMetadata('my-group', 7, 'member-1', 'instance-1'),
            'test',
            11
        );

        self::assertSame(3, $request->getApiVersion(), 'Kafka 2.5 raised the api to the version 3 of KIP-447');
        self::assertTrue(TxnOffsetCommitRequestV3::isFlexible(), 'which is the first flexible one as well');

        $hex = bin2hex((string) $request);

        // The three fields the version added, behind the producer epoch: the generation, the compact member id
        // and the compact nullable instance id of a static member
        self::assertStringContainsString(
            '0000002a' . '0003' . '00000007' . '09' . bin2hex('member-1') . '0b' . bin2hex('instance-1'),
            $hex
        );
        self::assertSame($hex, bin2hex((string) TxnOffsetCommitRequestV3::unpack(new StringStream((string) $request))));
    }

    /**
     * A producer that is not a member of the group sends the generation -1 with the empty member id
     */
    public function testAProducerThatIsNoMemberSendsTheGenerationMinusOneAndAnEmptyMemberId(): void
    {
        $request = new TxnOffsetCommitRequest(
            'tx-1',
            'my-group',
            42,
            3,
            ['topic' => [0 => new OffsetAndMetadata(17)]],
            null,
            'test',
            11
        );

        self::assertStringContainsString(
            '0000002a' . '0003' . 'ffffffff' . '01' . '00',
            bin2hex((string) $request),
            'the generation -1, the empty compact member id and the compact null of the instance id'
        );
    }

    /**
     * The three codes of KIP-447, as the coordinator answered them on the container
     */
    public function testTheVersionThreeAnswersTheGenerationAndMemberChecksPerPartition(): void
    {
        $answers = [
            KafkaException::NO_ERROR          => '000000220000038e0000000000020e74342d32352d766563746f727302000000000000000000',
            KafkaException::ILLEGAL_GENERATION => '000000220000038f0000000000020e74342d32352d766563746f727302000000000016000000',
            KafkaException::UNKNOWN_MEMBER_ID => '00000022000003900000000000020e74342d32352d766563746f727302000000000019000000',
        ];

        foreach ($answers as $errorCode => $hex) {
            $response = TxnOffsetCommitResponse::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($errorCode, $response->topics['t4-25-vectors']->partitions[0]->errorCode);
            self::assertSame($hex, bin2hex((string) $response), 'and it survives a round trip');
        }
    }

    /**
     * The answers of version 0 are read by the classes of their own version, which have the same layout
     */
    public function testTheVersionZeroAnswersAreReadByTheClassesOfTheirOwnVersion(): void
    {
        $answers = [
            AddPartitionsToTxnResponseV0::class => self::ADD_PARTITIONS_RESPONSE_HEX,
            AddOffsetsToTxnResponseV0::class    => self::ADD_OFFSETS_RESPONSE_HEX,
            EndTxnResponseV0::class             => self::END_TXN_RESPONSE_HEX,
            TxnOffsetCommitResponseV0::class    => self::TXN_OFFSET_COMMIT_RESPONSE_HEX,
        ];

        foreach ($answers as $class => $hex) {
            $response = $class::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($hex, bin2hex((string) $response), "{$class} does not survive a round trip");
        }
    }

    /**
     * Kafka 3.8, KIP-890: the four version bumps of this file declare no field at all
     *
     * "adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" is the whole comment of every one of them
     * in the message specifications @ 3.8.1, so the frame of the new version has to be the frame of the version
     * below it with another number in the header - which is what the keep-behind classes are compared against.
     */
    public function testTheKip890VersionsOfKafka38AreTheFramesBelowThemWithAHigherVersionField(): void
    {
        $frames = [
            'AddPartitionsToTxn' => [
                new AddPartitionsToTxnRequest('tx-1', 42, 3, ['topic' => [0, 1]], 'test', 7, true),
                new AddPartitionsToTxnRequestV4('tx-1', 42, 3, ['topic' => [0, 1]], 'test', 7, true),
                5,
            ],
            'AddOffsetsToTxn' => [
                new AddOffsetsToTxnRequest('tx-1', 42, 3, 'my-group', 'test', 7),
                new AddOffsetsToTxnRequestV3('tx-1', 42, 3, 'my-group', 'test', 7),
                4,
            ],
            'EndTxn' => [
                new EndTxnRequestV4('tx-1', 42, 3, EndTxnRequest::COMMIT, 'test', 7),
                new EndTxnRequestV3('tx-1', 42, 3, EndTxnRequest::COMMIT, 'test', 7),
                4,
            ],
            'TxnOffsetCommit' => [
                new TxnOffsetCommitRequestV4('tx-1', 'my-group', 42, 3, ['topic' => [0 => 17]], null, 'test', 7),
                new TxnOffsetCommitRequestV3('tx-1', 'my-group', 42, 3, ['topic' => [0 => 17]], null, 'test', 7),
                4,
            ],
        ];

        foreach ($frames as $api => [$current, $keptBehind, $version]) {
            self::assertSame($version, $current->getApiVersion(), "{$api} is at the version Kafka 3.8 added");
            self::assertSame($version - 1, $keptBehind->getApiVersion(), "{$api} keeps the version below it");

            $new = bin2hex((string) $current);
            $old = bin2hex((string) $keptBehind);

            // The version field is the third int16 of the request header, bytes 8 and 9 of the frame
            self::assertSame(
                substr($old, 0, 12) . sprintf('%04x', $version) . substr($old, 16),
                $new,
                "the {$api} frame of Kafka 3.8 differs from the one below it in the version field alone"
            );
        }
    }

    /**
     * And so do the answers, which none of the four versions touches either
     */
    public function testTheKip890AnswersOfKafka38AreReadByTheClassesOfTheVersionBelowThem(): void
    {
        $answers = [
            AddPartitionsToTxnResponse::class => [AddPartitionsToTxnResponseV4::class, self::ADD_PARTITIONS_V4_RESPONSE_HEX],
            AddOffsetsToTxnResponse::class    => [AddOffsetsToTxnResponseV3::class, '0000000c000000070000000000000000'],
            EndTxnResponseV4::class           => [EndTxnResponseV3::class, '0000000c000000070000000000003000'],
            TxnOffsetCommitResponseV4::class  => [
                TxnOffsetCommitResponseV3::class,
                '0000001a' . '00000007' . '00' . '00000000' . '02' . '06746f706963' . '02' . '00000000' . '0019' . '00' . '00' . '00',
            ],
        ];

        foreach ($answers as $current => [$keptBehind, $hex]) {
            $new = $current::unpack(new StringStream((string) hex2bin($hex)));
            $old = $keptBehind::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame($hex, bin2hex((string) $new), "{$current} does not survive a round trip");
            self::assertSame(bin2hex((string) $old), bin2hex((string) $new), 'both versions read the same bytes');
        }
    }

    /**
     * The version 5 of AddPartitionsToTxn is a broker version as the version 4 is, so no client method sends it
     */
    public function testTheAddPartitionsToTxnVersionFiveStaysABrokerVersion(): void
    {
        self::assertSame(5, AddPartitionsToTxnRequest::VERSION, 'the class of the version Kafka 3.8 added');
        self::assertSame(3, AddPartitionsToTxnRequestV3::VERSION, 'the version Client::addPartitionsToTxn() sends');
        self::assertSame(
            AddPartitionsToTxnRequest::MIN_BATCHED_VERSION,
            4,
            'the batch arrived with the version 4 and the version 5 keeps it'
        );

        $batch = AddPartitionsToTxnRequest::forTransactions(
            [new AddPartitionsToTxnTransaction('tx-1', 42, 3, ['topic' => [0]], true)],
            'test',
            7
        );

        self::assertSame(5, $batch->getApiVersion());
        self::assertStringStartsWith('0000002f' . '0018' . '0005', bin2hex((string) $batch));

        $this->expectException(UnsupportedVersionException::class);
        AddPartitionsToTxnRequestV3::forTransactions(
            [
                new AddPartitionsToTxnTransaction('tx-1', 42, 3, ['topic' => [0]]),
                new AddPartitionsToTxnTransaction('tx-2', 43, 1, ['topic' => [0]]),
            ],
            'test',
            7
        );
    }

    /**
     * EndTxn v5 and TxnOffsetCommit v5 (Kafka 4.0, KIP-890 part 2) declare no field of the request
     *
     * "Version 5 enables bumping epoch on every transaction" and "Version 5 is the same as version 4" are the
     * comments of `EndTxnRequest.json` and `TxnOffsetCommitRequest.json` @ 4.0.0: the version is the switch of the
     * transaction protocol v2, the frame is the one of the version 4 with another number in the header.
     */
    public function testTheKip890VersionsOfKafka40AreTheFramesOfTheVersionFourWithAHigherVersionField(): void
    {
        $frames = [
            'EndTxn' => [
                new EndTxnRequest('tx-1', 42, 3, EndTxnRequest::ABORT, 'test', 7),
                new EndTxnRequestV4('tx-1', 42, 3, EndTxnRequest::ABORT, 'test', 7),
            ],
            'TxnOffsetCommit' => [
                new TxnOffsetCommitRequest('tx-1', 'my-group', 42, 3, ['topic' => [0 => 17]], null, 'test', 7),
                new TxnOffsetCommitRequestV4('tx-1', 'my-group', 42, 3, ['topic' => [0 => 17]], null, 'test', 7),
            ],
        ];

        foreach ($frames as $api => [$current, $keptBehind]) {
            self::assertSame(5, $current->getApiVersion(), "{$api} is at the version Kafka 4.0 added");
            self::assertSame(4, $keptBehind->getApiVersion(), "{$api} keeps the version of the protocol v1");

            $old = bin2hex((string) $keptBehind);
            self::assertSame(
                substr($old, 0, 12) . '0005' . substr($old, 16),
                bin2hex((string) $current),
                "the {$api} frame of Kafka 4.0 differs from the version 4 in the version field alone"
            );
        }
    }

    /**
     * The answer of EndTxn v5 carries the producer id and the epoch of the next transaction
     *
     * `EndTxnResponse.json` @ 4.0.0 appends `producer_id` (int64) and `producer_epoch` (int16) behind the error
     * code, both with the default -1; the node answers the -1 pair with every error code.
     */
    public function testTheVersionFiveAnswerOfEndTxnCarriesTheBumpedProducerIdAndEpoch(): void
    {
        $bumped = EndTxnResponse::unpack(new StringStream((string) hex2bin(
            '00000016' . '00000065' . '00' . '00000000' . '0000' . '00000000000000d3' . '0001' . '00'
        )));

        self::assertSame(KafkaException::NO_ERROR, $bumped->errorCode);
        self::assertTrue($bumped->hasProducerIdAndEpoch());
        self::assertSame(211, $bumped->producerId);
        self::assertSame(1, $bumped->producerEpoch);

        $refused = EndTxnResponse::unpack(new StringStream((string) hex2bin(
            '00000016' . '00000066' . '00' . '00000000' . '0033' . 'ffffffffffffffff' . 'ffff' . '00'
        )));

        self::assertSame(KafkaException::CONCURRENT_TRANSACTIONS, $refused->errorCode);
        self::assertFalse($refused->hasProducerIdAndEpoch(), 'the defaults -1/-1 hand out nothing');
        self::assertSame(-1, $refused->producerEpoch);

        $versionFour = EndTxnResponseV4::unpack(new StringStream((string) hex2bin('0000000c000000070000000000003000')));

        self::assertFalse($versionFour->hasProducerIdAndEpoch(), 'the version 4 has no place for the pair');
        self::assertSame('0000000c000000070000000000003000', bin2hex((string) $versionFour));
    }

    /**
     * The answer of TxnOffsetCommit v5 is the one of the version 4, which reads the same bytes
     */
    public function testTheVersionFiveAnswerOfTxnOffsetCommitIsTheFrameOfTheVersionFour(): void
    {
        $hex = '0000001a' . '00000007' . '00' . '00000000' . '02' . '06746f706963' . '02' . '00000000' . '0078'
            . '00' . '00' . '00';

        $new = TxnOffsetCommitResponse::unpack(new StringStream((string) hex2bin($hex)));
        $old = TxnOffsetCommitResponseV4::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(5, $new::VERSION);
        self::assertSame(120, $new->topics['topic']->partitions[0]->errorCode);
        self::assertSame($hex, bin2hex((string) $new));
        self::assertSame(bin2hex((string) $old), bin2hex((string) $new));
    }

    /**
     * Puts the size field in front of the hex dump of a frame
     */
    private static function sized(string $hex): string
    {
        return sprintf('%08x', strlen($hex) / 2) . $hex;
    }
}
