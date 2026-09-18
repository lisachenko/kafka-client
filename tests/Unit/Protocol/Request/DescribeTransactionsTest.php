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
use Protocol\Kafka\Admin\TransactionDescription;
use Protocol\Kafka\Admin\TransactionState;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeTransactionsResponseTopic;
use Protocol\Kafka\Protocol\Data\DescribeTransactionsResponseTransactionState;
use Protocol\Kafka\Protocol\Request\DescribeTransactionsRequest;
use Protocol\Kafka\Protocol\Request\DescribeTransactionsResponse;

/**
 * Byte-exact tests for DescribeTransactions (key 65, v0), the api Kafka 3.0 added next to DescribeProducers.
 *
 * The api is flexible from its version 0, so every string and every array of both frames is compact and every
 * structure ends in a tagged-field section. The answer has **no top-level error code**: every transactional id of
 * the request carries one of its own, and an entry that carries an error carries the defaults of the
 * specification behind it.
 *
 * @see docs/protocol/3.9.md, section "DescribeTransactions API (key 65, v0)"
 */
#[CoversClass(DescribeTransactionsRequest::class)]
#[CoversClass(DescribeTransactionsResponse::class)]
#[CoversClass(DescribeTransactionsResponseTransactionState::class)]
#[CoversClass(DescribeTransactionsResponseTopic::class)]
#[CoversClass(TransactionDescription::class)]
#[CoversClass(TransactionState::class)]
final class DescribeTransactionsTest extends TestCase
{
    /**
     * A request for two transactional ids.
     *
     *   Size          => 00 00 00 1b (27 bytes)
     *   ApiKey        => 00 41 (65), ApiVersion => 00 00
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test", TAG_BUFFER => 00
     *   TransactionalIds => 03 (2 + 1), 05 "tx-a", 05 "tx-b"
     *   TAG_BUFFER    => 00
     */
    private const string REQUEST_HEX = '0000001b'
        . '0041'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '03'
        . '05' . '74782d61'
        . '05' . '74782d62'
        . '00';

    /**
     * An answer with one open transaction and one id the coordinator does not know.
     *
     *   Size             => 00 00 00 5f (95 bytes)
     *   CorrelationId    => 00 00 00 09, TAG_BUFFER => 00
     *   ThrottleTimeMs   => 00 00 00 00
     *   TransactionStates => 03 (2 + 1)
     *     [0] ErrorCode 00 00, TransactionalId 05 "tx-a", TransactionState 08 "Ongoing",
     *         TransactionTimeoutMs 00 00 ea 60, TransactionStartTimeMs 00 00 01 7e 12 ef 9c 00,
     *         ProducerId 00 00 00 00 00 00 01 52, ProducerEpoch 00 03,
     *         Topics 02 -> 07 "events" 02 -> 00 00 00 00, TAG_BUFFER 00, TAG_BUFFER 00
     *     [1] ErrorCode 00 69 (105), TransactionalId 05 "tx-b", the defaults and an empty topic array
     *   TAG_BUFFER       => 00
     */
    private const string RESPONSE_HEX = '0000005f'
        . '00000009'
        . '00'
        . '00000000'
        . '03'
        . '0000'
        . '05' . '74782d61'
        . '08' . '4f6e676f696e67'
        . '0000ea60'
        . '0000017e12ef9c00'
        . '0000000000000152'
        . '0003'
        . '02'
        . '07' . '6576656e7473'
        . '02' . '00000000'
        . '00'
        . '00'
        . '0069'
        . '05' . '74782d62'
        . '01'
        . '00000000'
        . '0000000000000000'
        . '0000000000000000'
        . '0000'
        . '01'
        . '00'
        . '00';

    public function testTheRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DescribeTransactionsRequest(['tx-a', 'tx-b'], 'test', 9);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_TRANSACTIONS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertTrue(DescribeTransactionsRequest::isFlexible(), 'the api is flexible from its version 0');
        self::assertSame(DescribeTransactionsRequest::HEADER_V2, $request->getHeaderVersion());
        self::assertSame(['tx-a', 'tx-b'], $request->getTransactionalIds());
    }

    /**
     * An empty id array is a request about nothing, and the shortest frame of the api
     */
    public function testAnEmptyIdArrayIsTheShortestFrameOfTheApi(): void
    {
        $empty = new DescribeTransactionsRequest([], 'test', 9);

        self::assertSame(
            '00000011' . '0041' . '0000' . '00000009' . '0004' . '74657374' . '00' . '01' . '00',
            bin2hex((string) $empty),
            'the compact length 01 of an empty array, not the null of a nullable one'
        );
    }

    public function testTheAnswerCarriesOneEntryPerIdWithItsOwnErrorCode(): void
    {
        $response = DescribeTransactionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['tx-a', 'tx-b'], array_keys($response->transactionStates), 'indexed by the id');

        $open = $response->transactionStates['tx-a'];

        self::assertSame(KafkaException::NO_ERROR, $open->errorCode);
        self::assertSame('Ongoing', $open->transactionState);
        self::assertSame(60000, $open->transactionTimeoutMs);
        self::assertSame(1640995200000, $open->transactionStartTimeMs);
        self::assertSame(338, $open->producerId);
        self::assertSame(3, $open->producerEpoch, 'an int16 here, an int32 in a DescribeProducers answer');
        self::assertSame(['events'], array_keys($open->topics));
        self::assertSame([0], $open->topics['events']->partitions);

        $unknown = $response->transactionStates['tx-b'];

        self::assertSame(
            KafkaException::TRANSACTIONAL_ID_NOT_FOUND,
            $unknown->errorCode,
            'the code Kafka 3.0 added for an id the coordinator has no state for'
        );
        self::assertSame('', $unknown->transactionState, 'an entry with an error carries the defaults behind it');
        self::assertSame(0, $unknown->transactionStartTimeMs, 'the 0 of the specification, not the -1 of `Empty`');
        self::assertSame([], $unknown->topics);
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response), 'and the frame survives the round trip');
    }

    /**
     * The state names of the coordinator are the cases of the value object, and an unknown one is not an error
     */
    public function testTheStateNamesAreParsedAndAnUnknownOneFoldsIntoUnknown(): void
    {
        self::assertSame(TransactionState::Ongoing, TransactionState::fromWire('Ongoing'));
        self::assertSame(TransactionState::PrepareEpochFence, TransactionState::fromWire('PrepareEpochFence'));
        self::assertSame(TransactionState::Dead, TransactionState::fromWire('Dead'));
        self::assertSame(
            TransactionState::Unknown,
            TransactionState::fromWire('ongoing'),
            'the names are matched verbatim, as the coordinator matches a state filter'
        );
        self::assertSame(TransactionState::Unknown, TransactionState::fromWire(''));

        self::assertTrue(TransactionState::Ongoing->isInFlight());
        self::assertTrue(TransactionState::PrepareAbort->isInFlight());
        self::assertTrue(TransactionState::PrepareCommit->isInFlight());
        self::assertFalse(TransactionState::Empty->isInFlight());
        self::assertFalse(TransactionState::CompleteCommit->isInFlight());
    }

    /**
     * The -1 of the wire is `null` in the value object, as it is for the transaction offset of a producer state
     */
    public function testTheDescriptionModelsTheAbsentStartTimeAsNull(): void
    {
        $open = new TransactionDescription(
            1,
            TransactionState::Ongoing,
            338,
            3,
            60000,
            1640995200000,
            [new TopicPartition('events', 0)]
        );
        $idle = new TransactionDescription(1, TransactionState::Empty, 338, 3, 60000, null, []);

        self::assertTrue($open->hasOpenTransaction());
        self::assertSame(1640995200000, $open->transactionStartTimeMs);
        self::assertSame('events-0', (string) $open->topicPartitions[0]);
        self::assertFalse($idle->hasOpenTransaction());
        self::assertNull($idle->transactionStartTimeMs);
        self::assertSame([], $idle->topicPartitions);
    }
}
