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
use Protocol\Kafka\Admin\TransactionListing;
use Protocol\Kafka\Admin\TransactionState;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ListTransactionsResponseTransactionState;
use Protocol\Kafka\Protocol\Request\ListTransactionsRequest;
use Protocol\Kafka\Protocol\Request\ListTransactionsRequestV0;
use Protocol\Kafka\Protocol\Request\ListTransactionsResponse;
use Protocol\Kafka\Protocol\Request\ListTransactionsResponseV0;

/**
 * Byte-exact tests for ListTransactions (key 66, v0 and v1), the second api Kafka 3.0 added to the admin surface.
 *
 * Unlike DescribeTransactions (65) this one has a **top-level error code**, and it stands behind the throttle
 * time; the `unknown_state_filters` next to it name the state filters of the request the coordinator could not
 * resolve, which is a report and not an error.
 *
 * **Version 1 (Kafka 3.8, KIP-994)** appends the `duration_filter` int64 to the request and changes the answer in
 * nothing at all, which is why both versions of the answer are the same bytes read through two classes.
 *
 * @see docs/protocol/3.9.md, section "ListTransactions API (key 66, v0 and v1)"
 */
#[CoversClass(ListTransactionsRequest::class)]
#[CoversClass(ListTransactionsRequestV0::class)]
#[CoversClass(ListTransactionsResponse::class)]
#[CoversClass(ListTransactionsResponseV0::class)]
#[CoversClass(ListTransactionsResponseTransactionState::class)]
#[CoversClass(TransactionListing::class)]
#[CoversClass(TransactionState::class)]
final class ListTransactionsTest extends TestCase
{
    /**
     * A version 0 request with both filters.
     *
     *   Size          => 00 00 00 22 (34 bytes)
     *   ApiKey        => 00 42 (66), ApiVersion => 00 00
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test", TAG_BUFFER => 00
     *   StateFilters      => 02 (1 + 1), 08 "Ongoing"
     *   ProducerIdFilters => 02 (1 + 1), 00 00 00 00 00 00 01 52 (338)
     *   TAG_BUFFER    => 00
     */
    private const string REQUEST_HEX_V0 = '00000022'
        . '0042'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '02' . '08' . '4f6e676f696e67'
        . '02' . '0000000000000152'
        . '00';

    /**
     * The same request at version 1: eight bytes longer, the `duration_filter` behind the producer ids.
     *
     *   Size           => 00 00 00 2a (42 bytes)
     *   ApiKey         => 00 42 (66), ApiVersion => 00 01
     *   DurationFilter => 00 00 00 00 00 00 75 30 (30000 ms)
     */
    private const string REQUEST_HEX_V1 = '0000002a'
        . '0042'
        . '0001'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '02' . '08' . '4f6e676f696e67'
        . '02' . '0000000000000152'
        . '0000000000007530'
        . '00';

    /**
     * An answer with one transaction and one state filter the coordinator did not know.
     *
     *   Size                => 00 00 00 2a (42 bytes)
     *   CorrelationId       => 00 00 00 09, TAG_BUFFER => 00
     *   ThrottleTimeMs      => 00 00 00 00
     *   ErrorCode           => 00 00
     *   UnknownStateFilters => 02 (1 + 1), 06 "bogus"
     *   TransactionStates   => 02 (1 + 1)
     *     [0] TransactionalId 05 "tx-a", ProducerId 00 00 00 00 00 00 01 52, TransactionState 08 "Ongoing",
     *         TAG_BUFFER 00
     *   TAG_BUFFER          => 00
     */
    private const string RESPONSE_HEX = '0000002a'
        . '00000009'
        . '00'
        . '00000000'
        . '0000'
        . '02' . '06' . '626f677573'
        . '02'
        . '05' . '74782d61'
        . '0000000000000152'
        . '08' . '4f6e676f696e67'
        . '00'
        . '00';

    public function testTheVersion0RequestCarriesBothFilters(): void
    {
        $request = new ListTransactionsRequestV0(['Ongoing'], [338], 'test', 9);

        self::assertSame(self::REQUEST_HEX_V0, bin2hex((string) $request));
        self::assertSame(ApiKeys::LIST_TRANSACTIONS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertTrue(ListTransactionsRequestV0::isFlexible());
        self::assertSame(ListTransactionsRequestV0::HEADER_V2, $request->getHeaderVersion());
        self::assertSame(['Ongoing'], $request->getStateFilters());
        self::assertSame([338], $request->getProducerIdFilters());
    }

    /**
     * Version 1 (KIP-994) is the version 0 body with the `duration_filter` int64 appended to it
     */
    public function testTheVersion1RequestAppendsTheDurationFilter(): void
    {
        $request = new ListTransactionsRequest(['Ongoing'], [338], 'test', 9, 30000);

        self::assertSame(self::REQUEST_HEX_V1, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion(), 'the client sends the duration filter from Kafka 3.8 on');
        self::assertSame(30000, $request->getDurationFilter());
        self::assertSame(
            strlen(self::REQUEST_HEX_V0) / 2 + 8,
            strlen(self::REQUEST_HEX_V1) / 2,
            'the only difference of the two frames is the int64 of the filter'
        );
    }

    /**
     * Both filters are empty by default, which is the request for every transaction of the broker
     */
    public function testAnUnfilteredRequestIsTwoEmptyArraysAndTheFilterMinusOne(): void
    {
        $request = new ListTransactionsRequest([], [], 'test', 9);

        self::assertSame(
            '0000001a' . '0042' . '0001' . '00000009' . '0004' . '74657374' . '00' . '01' . '01'
            . 'ffffffffffffffff' . '00',
            bin2hex((string) $request),
            'an empty filter is the compact length 01, and the -1 duration means "every transaction"'
        );
        self::assertSame(ListTransactionsRequest::NO_DURATION_FILTER, $request->getDurationFilter());
        self::assertSame([], $request->getStateFilters());
        self::assertSame([], $request->getProducerIdFilters());

        self::assertSame(
            '00000012' . '0042' . '0000' . '00000009' . '0004' . '74657374' . '00' . '01' . '01' . '00',
            bin2hex((string) new ListTransactionsRequestV0([], [], 'test', 9)),
            'the version below has nowhere to put the filter at all'
        );
    }

    public function testTheAnswerCarriesTheTopLevelCodeBehindTheThrottleTime(): void
    {
        $response = ListTransactionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(
            ['bogus'],
            $response->unknownStateFilters,
            'a state filter the coordinator does not know is reported, not refused'
        );
        self::assertSame(['tx-a'], array_keys($response->transactionStates), 'indexed by the transactional id');

        $transaction = $response->transactionStates['tx-a'];

        self::assertSame('tx-a', $transaction->transactionalId);
        self::assertSame(338, $transaction->producerId);
        self::assertSame('Ongoing', $transaction->transactionState);
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * The answer of version 1 is the answer of version 0, byte for byte (KIP-994 touched the request alone)
     */
    public function testBothVersionsOfTheAnswerAreTheSameBytes(): void
    {
        $atVersion1 = ListTransactionsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $atVersion0 = ListTransactionsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, ListTransactionsResponse::VERSION);
        self::assertSame(0, ListTransactionsResponseV0::VERSION);
        self::assertSame(
            ListTransactionsResponse::getScheme(),
            ListTransactionsResponseV0::getScheme(),
            'the two versions declare the same fields'
        );
        self::assertSame(bin2hex((string) $atVersion1), bin2hex((string) $atVersion0));
        self::assertSame(
            array_keys($atVersion1->transactionStates),
            array_keys($atVersion0->transactionStates)
        );
    }

    /**
     * The listing carries three fields, where a DescribeTransactions entry carries eight
     */
    public function testTheListingIsTheThreeFieldsOfTheAnswer(): void
    {
        $listing = new TransactionListing('tx-a', 338, TransactionState::Ongoing);

        self::assertSame('tx-a', $listing->transactionalId);
        self::assertSame(338, $listing->producerId);
        self::assertSame(TransactionState::Ongoing, $listing->state);
        self::assertSame('Ongoing', $listing->state->value, 'the value is the name that travels on the wire');
    }
}
