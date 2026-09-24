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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\TransactionState;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidRegularExpressionException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\ListTransactionsRequest;
use Protocol\Kafka\Protocol\Request\ListTransactionsRequestV1;
use Protocol\Kafka\Protocol\Request\ListTransactionsResponse;
use Protocol\Kafka\Protocol\Request\ListTransactionsResponseV1;

/**
 * Exercises the `transactional_id_pattern` of ListTransactions v2 (key 66, Kafka 4.1, KIP-1152) on the 4.3.1 node.
 *
 * The pattern is a regular expression of RE2/J that the **whole** transactional id has to match, ANDed with the
 * three filters of the versions below; a pattern the coordinator cannot compile is the 128
 * (`InvalidRegularExpression`) of Kafka 4.0. The class creates two transactional ids of its own - an
 * `InitProducerId` each, which leaves them `Empty` - and asks for them by pattern; the coordinator expires them with
 * `transactional.id.expiration.ms`, so there is nothing to clean up.
 *
 * @see docs/protocol/4.3.md, section "The transactional id pattern of KIP-1152 (v2)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ListTransactionsRequest::class)]
#[CoversClass(ListTransactionsRequestV1::class)]
#[CoversClass(ListTransactionsResponse::class)]
#[CoversClass(ListTransactionsResponseV1::class)]
final class TransactionIdPatternApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t1-41-pattern';

    /**
     * The prefix of this run's transactional ids, unique per run
     */
    private static ?string $prefix = null;

    /**
     * Producer id of every transactional id of this run
     *
     * @var array<string, int>
     */
    private static array $producerIds = [];

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $cluster     = Cluster::bootstrap($this->configuration());
        $this->admin = new AdminClient($cluster, $this->configuration());

        if (self::$prefix === null) {
            self::$prefix = 't1-41-pattern-' . bin2hex(random_bytes(4));
            $client       = new Client($cluster, $this->configuration());
            foreach (['a', 'b'] as $suffix) {
                $transactionalId = self::$prefix . '-' . $suffix;
                $manager         = new TransactionManager($client, $transactionalId, 60000, $this->configuration());
                $manager->initTransactions();
                self::$producerIds[$transactionalId] = $manager->getProducerIdAndEpoch()->producerId;
            }
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$prefix      = null;
        self::$producerIds = [];

        parent::tearDownAfterClass();
    }

    /**
     * A pattern that matches the ids of this run lists both of them, and nothing else
     */
    public function testAPatternListsTheIdsItMatches(): void
    {
        $listed = $this->admin->listTransactions(transactionalIdPattern: preg_quote((string) self::$prefix) . '-.*');

        self::assertSame(array_keys(self::$producerIds), $this->sorted(array_keys($listed)));
        foreach ($listed as $transactionalId => $listing) {
            self::assertSame(self::$producerIds[$transactionalId], $listing->producerId);
            self::assertSame(TransactionState::Empty, $listing->state, 'an InitProducerId and nothing else');
        }

        self::assertSame(
            [self::$prefix . '-b'],
            array_keys($this->admin->listTransactions(transactionalIdPattern: '.*' . self::$prefix . '-[b-z]')),
            'a character class, as RE2 knows it'
        );
    }

    /**
     * The whole id has to match: a prefix without a wildcard matches nothing (`matches()`, not `find()`)
     */
    public function testThePatternHasToMatchTheWholeId(): void
    {
        self::assertSame([], $this->admin->listTransactions(transactionalIdPattern: (string) self::$prefix));
    }

    /**
     * The pattern is ANDed with the other filters, and the empty pattern is "every id", as a null one is
     */
    public function testThePatternIsAndedWithTheOtherFilters(): void
    {
        $idA = self::$prefix . '-a';

        self::assertSame(
            [$idA],
            array_keys($this->admin->listTransactions(
                [TransactionState::Empty],
                [self::$producerIds[$idA]],
                transactionalIdPattern: '.*'
            ))
        );
        self::assertSame(
            [],
            $this->admin->listTransactions(
                [TransactionState::Ongoing],
                [self::$producerIds[$idA]],
                transactionalIdPattern: '.*'
            ),
            'an Empty id is not Ongoing, whatever the pattern'
        );
        self::assertSame(
            [$idA],
            array_keys($this->admin->listTransactions([], [self::$producerIds[$idA]], transactionalIdPattern: '')),
            'the empty pattern is not applied at all'
        );
    }

    /**
     * A pattern RE2/J cannot compile is refused with the 128 of Kafka 4.0, and a pattern RE2 does not have is one
     */
    public function testAnInvalidPatternIsTheInvalidRegularExpression(): void
    {
        foreach (['t1-41-(unclosed', 't1-41-(?=lookahead)'] as $pattern) {
            try {
                $this->admin->listTransactions(transactionalIdPattern: $pattern);
                self::fail("{$pattern} is not a valid RE2 expression");
            } catch (InvalidRegularExpressionException $exception) {
                self::assertSame(KafkaException::INVALID_REGULAR_EXPRESSION, $exception->getCode());
                self::assertSame($pattern, $exception->getContext()['transactionalIdPattern']);
            }
        }
    }

    /**
     * A version 1 frame has no field for the pattern, and lists what the other filters let through
     */
    public function testTheVersionOneFrameCannotAskForAPattern(): void
    {
        $idA    = self::$prefix . '-a';
        $stream = $this->connect();

        new ListTransactionsRequestV1([], [self::$producerIds[$idA]], self::CLIENT_ID, 4901, -1, 'no-such-id')
            ->writeTo($stream);
        $atVersion1 = ListTransactionsResponseV1::unpack($stream);

        new ListTransactionsRequest([], [self::$producerIds[$idA]], self::CLIENT_ID, 4902, -1, 'no-such-id')
            ->writeTo($stream);
        $atVersion2 = ListTransactionsResponse::unpack($stream);

        self::assertSame([$idA], array_keys($atVersion1->transactionStates), 'the pattern never reached the wire');
        self::assertSame([], $atVersion2->transactionStates, 'and at version 2 it filters the id away');

        $stream->disconnect();
    }

    /**
     * @param list<string> $ids
     *
     * @return list<string>
     */
    private function sorted(array $ids): array
    {
        sort($ids);

        return $ids;
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
