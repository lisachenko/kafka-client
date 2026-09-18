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
use Protocol\Kafka\Protocol\Request\ListTransactionsResponse;

/**
 * Byte-exact tests for ListTransactions (key 66, v0), the second api Kafka 3.0 added to the admin surface.
 *
 * Unlike DescribeTransactions (65) this one has a **top-level error code**, and it stands behind the throttle
 * time; the `unknown_state_filters` next to it name the state filters of the request the coordinator could not
 * resolve, which is a report and not an error.
 *
 * @see docs/protocol/3.9.md, section "ListTransactions API (key 66, v0)"
 */
#[CoversClass(ListTransactionsRequest::class)]
#[CoversClass(ListTransactionsResponse::class)]
#[CoversClass(ListTransactionsResponseTransactionState::class)]
#[CoversClass(TransactionListing::class)]
#[CoversClass(TransactionState::class)]
final class ListTransactionsTest extends TestCase
{
    /**
     * A request with both filters.
     *
     *   Size          => 00 00 00 22 (34 bytes)
     *   ApiKey        => 00 42 (66), ApiVersion => 00 00
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test", TAG_BUFFER => 00
     *   StateFilters      => 02 (1 + 1), 08 "Ongoing"
     *   ProducerIdFilters => 02 (1 + 1), 00 00 00 00 00 00 01 52 (338)
     *   TAG_BUFFER    => 00
     */
    private const string REQUEST_HEX = '00000022'
        . '0042'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '02' . '08' . '4f6e676f696e67'
        . '02' . '0000000000000152'
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

    public function testTheRequestCarriesBothFilters(): void
    {
        $request = new ListTransactionsRequest(['Ongoing'], [338], 'test', 9);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::LIST_TRANSACTIONS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'the v1 duration filter is Kafka 3.8, not this milestone');
        self::assertTrue(ListTransactionsRequest::isFlexible());
        self::assertSame(ListTransactionsRequest::HEADER_V2, $request->getHeaderVersion());
        self::assertSame(['Ongoing'], $request->getStateFilters());
        self::assertSame([338], $request->getProducerIdFilters());
    }

    /**
     * Both filters are empty by default, which is the request for every transaction of the broker
     */
    public function testAnUnfilteredRequestIsTwoEmptyArrays(): void
    {
        $request = new ListTransactionsRequest([], [], 'test', 9);

        self::assertSame(
            '00000012' . '0042' . '0000' . '00000009' . '0004' . '74657374' . '00' . '01' . '01' . '00',
            bin2hex((string) $request),
            'an empty filter is the compact length 01, and it means "everything"'
        );
        self::assertSame([], $request->getStateFilters());
        self::assertSame([], $request->getProducerIdFilters());
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
