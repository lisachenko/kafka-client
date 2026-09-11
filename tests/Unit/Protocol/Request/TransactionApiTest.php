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
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResponsePartition;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResponseTopic;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestPartition;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitResponsePartition;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitResponseTopic;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersRequestMarker;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersResponseMarker;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersResponsePartition;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersResponseTopic;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequestV0;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponse;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponseV0;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequestV0;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponse;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponseV0;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;
use Protocol\Kafka\Protocol\Request\EndTxnRequestV0;
use Protocol\Kafka\Protocol\Request\EndTxnResponse;
use Protocol\Kafka\Protocol\Request\EndTxnResponseV0;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV1;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV0;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV1;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersRequest;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersResponse;

/**
 * Byte-exact tests for the five transaction APIs of Kafka 0.11 (api keys 24 to 28, v0 each).
 *
 * @see docs/protocol/2.8.md, sections "AddPartitionsToTxn API (key 24, v0 and v1)", "AddOffsetsToTxn API (key 25, v0 and v1)",
 *      "EndTxn API (key 26, v0 and v1)", "WriteTxnMarkers API (key 27, v0)" and "TxnOffsetCommit API (key 28, v0 to v2)"
 */
#[CoversClass(AddPartitionsToTxnRequest::class)]
#[CoversClass(AddPartitionsToTxnRequestV0::class)]
#[CoversClass(AddPartitionsToTxnResponse::class)]
#[CoversClass(AddPartitionsToTxnResponseV0::class)]
#[CoversClass(AddPartitionsToTxnResponseTopic::class)]
#[CoversClass(AddPartitionsToTxnResponsePartition::class)]
#[CoversClass(AddOffsetsToTxnRequest::class)]
#[CoversClass(AddOffsetsToTxnRequestV0::class)]
#[CoversClass(AddOffsetsToTxnResponse::class)]
#[CoversClass(AddOffsetsToTxnResponseV0::class)]
#[CoversClass(EndTxnRequest::class)]
#[CoversClass(EndTxnRequestV0::class)]
#[CoversClass(EndTxnResponse::class)]
#[CoversClass(EndTxnResponseV0::class)]
#[CoversClass(WriteTxnMarkersRequest::class)]
#[CoversClass(WriteTxnMarkersResponse::class)]
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
        $request = new AddPartitionsToTxnRequest('tx-1', 42, 3, ['topic' => [0, 1]], 'test', 7);

        self::assertSame(self::ADD_PARTITIONS_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::ADD_PARTITIONS_TO_TXN, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'Kafka 2.0 raised the three apis to version 1');
    }

    public function testAnAlreadyBuiltTopicOfAddPartitionsToTxnIsTakenAsItIs(): void
    {
        $request = new AddPartitionsToTxnRequest(
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
        $response = AddPartitionsToTxnResponse::unpack(
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

    public function testAddOffsetsToTxnRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new AddOffsetsToTxnRequest('tx-1', 42, 3, 'my-group', 'test', 8);

        self::assertSame(self::ADD_OFFSETS_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::ADD_OFFSETS_TO_TXN, $request->getApiKey());
    }

    public function testAddOffsetsToTxnHasOneErrorCodeForTheWholeRequest(): void
    {
        $response = AddOffsetsToTxnResponse::unpack(
            new StringStream((string) hex2bin(self::ADD_OFFSETS_RESPONSE_HEX))
        );

        self::assertSame(100, $response->throttleTimeMs);
        self::assertSame(KafkaException::INVALID_TXN_STATE, $response->errorCode);
        self::assertSame(self::ADD_OFFSETS_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testTheTransactionResultIsTheOnlyDifferenceBetweenACommitAndAnAbort(): void
    {
        $commit = new EndTxnRequest('tx-1', 42, 3, EndTxnRequest::COMMIT, 'test', 9);
        $abort  = new EndTxnRequest('tx-1', 42, 3, EndTxnRequest::ABORT, 'test', 9);

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
        $response = EndTxnResponse::unpack(new StringStream((string) hex2bin(self::END_TXN_RESPONSE_HEX)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(KafkaException::INVALID_TXN_STATE, $response->errorCode);
        self::assertSame(self::END_TXN_RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testWriteTxnMarkersRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new WriteTxnMarkersRequest(
            [new WriteTxnMarkersRequestMarker(42, 3, EndTxnRequest::COMMIT, ['topic' => [0]], 7)],
            'test',
            10
        );

        self::assertSame(self::WRITE_MARKERS_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::WRITE_TXN_MARKERS, $request->getApiKey());
    }

    public function testTheWriteTxnMarkersAnswerIsTheOnlyOneOf011WithoutAThrottleTime(): void
    {
        $response = WriteTxnMarkersResponse::unpack(
            new StringStream((string) hex2bin(self::WRITE_MARKERS_RESPONSE_HEX))
        );

        // `WRITE_TXN_MARKERS_RESPONSE_V0` starts with the array: the api is broker-to-broker and is not throttled
        self::assertArrayNotHasKey('throttleTimeMs', WriteTxnMarkersResponse::getScheme());
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
        $request = new TxnOffsetCommitRequest(
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
        $request = new TxnOffsetCommitRequest('tx-1', 'my-group', 42, 3, ['topic' => [0 => 17]], 'test', 11);

        // The metadata is a nullable string, so an offset without one is the two bytes ff ff, behind the -1 of
        // the `committed_leader_epoch` that version 2 added
        self::assertStringEndsWith(
            '00000000' . '0000000000000011' . 'ffffffff' . 'ffff',
            bin2hex((string) $request)
        );
    }

    public function testTxnOffsetCommitReportsEveryErrorPerPartition(): void
    {
        $response = TxnOffsetCommitResponse::unpack(
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
        $request = new TxnOffsetCommitRequest(
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
        self::assertSame(-1, OffsetAndMetadata::NO_LEADER_EPOCH);
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

}
