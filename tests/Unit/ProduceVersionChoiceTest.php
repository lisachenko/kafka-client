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

namespace Protocol\Kafka\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV11;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\ClusterFixture;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\FakeClient;

/**
 * The version choice of Produce (Kafka 4.1): v13 outside a transaction and inside one of the transaction protocol v2,
 * v11 inside one of the protocol v1, v2 for a message set.
 *
 * `ProduceRequest.json` @ 4.0.0 on version 12: "Note when produce requests are used in transaction, if transaction V2
 * (KIP_890 part 2) is enabled, the produce request will also include the function for a AddPartitionsToTxn call. If
 * V2 is disabled, the client can't use produce request version higher than 11 within a transaction." The choice lives
 * in {@see Client::produceVersion()}, and the cap of a transactional producer is handed to it by
 * {@see Client::produce()}. Version 13 (Kafka 4.1, KIP-516) only names the topics by their ids, so it is sent wherever
 * version 12 was, and the cap of the protocol v1 stays at 11.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)" and "The transaction protocol v2 of KIP-890
 *      part 2 (v12)"
 */
#[CoversClass(Client::class)]
final class ProduceVersionChoiceTest extends TestCase
{
    private const string TOPIC = 'orders';

    public function testTheRecordBatchGoesOutAsVersionThirteen(): void
    {
        self::assertSame(13, Client::produceVersion(RecordBatch::MAGIC));
        self::assertSame(ProduceRequest::VERSION, Client::produceVersion(RecordBatch::MAGIC, null));
    }

    public function testACapLowersTheVersionOfTheRecordBatch(): void
    {
        self::assertSame(11, Client::produceVersion(RecordBatch::MAGIC, ProduceRequestV11::VERSION));
        self::assertSame(12, Client::produceVersion(RecordBatch::MAGIC, ProduceRequestV12::VERSION));
        self::assertSame(13, Client::produceVersion(RecordBatch::MAGIC, 99), 'a cap above the table changes nothing');
        self::assertSame(
            ProduceRequest::BASELINE_VERSION,
            Client::produceVersion(RecordBatch::MAGIC, 1),
            'and a cap below version 3 can not carry a record batch at all'
        );
    }

    public function testAMessageSetGoesOutAsVersionTwoWhateverTheCap(): void
    {
        self::assertSame(ProduceRequestV2::VERSION, Client::produceVersion(Message::MAGIC_V1));
        self::assertSame(ProduceRequestV2::VERSION, Client::produceVersion(Message::MAGIC_V0, 11));
    }

    public function testAPlainProducerIsNotCapped(): void
    {
        $client = $this->client();
        $client->produce([self::TOPIC => [0 => [new Record('plain')]]]);

        self::assertSame([null], $client->produceVersionCaps);
    }

    public function testAnIdempotentProducerWritesOutsideEveryTransactionAndIsNotCapped(): void
    {
        $client  = $this->client();
        $manager = new TransactionManager($client);

        $client->produce([self::TOPIC => [0 => [new Record('idempotent')]]], $manager);

        self::assertSame([null], $client->produceVersionCaps);
    }

    public function testATransactionalProducerThatWasNotInitializedIsCappedAtVersionEleven(): void
    {
        $client  = $this->client();
        $manager = new TransactionManager($client, 'orders-tx', 30000);

        $client->produce([self::TOPIC => [0 => [new Record('in a transaction')]]], $manager);

        self::assertFalse($manager->isTransactionV2Enabled());
        self::assertSame(
            [ProduceRequestV11::VERSION],
            $client->produceVersionCaps,
            'a v12 or v13 inside a transaction is the transaction protocol v2, which a producer of the v1 must not send'
        );
    }

    public function testATransactionalProducerOfTheProtocolV1IsCappedAtVersionEleven(): void
    {
        // A coordinator that finalizes `transaction.version` 1: the partitions are enrolled by AddPartitionsToTxn
        $client  = $this->client(1);
        $manager = new TransactionManager($client, 'orders-tx', 30000);
        $manager->initTransactions();
        $manager->beginTransaction();

        $client->produce([self::TOPIC => [0 => [new Record('protocol v1')]]], $manager);

        self::assertFalse($manager->isTransactionV2Enabled());
        self::assertSame([TransactionManager::LAST_PRODUCE_VERSION_BEFORE_TRANSACTION_V2], $client->produceVersionCaps);
        self::assertSame(11, TransactionManager::LAST_PRODUCE_VERSION_BEFORE_TRANSACTION_V2);
        self::assertSame(11, Client::produceVersion(RecordBatch::MAGIC, $client->produceVersionCaps[0]));
    }

    public function testATransactionalProducerOfTheProtocolV2SendsVersionThirteen(): void
    {
        // A coordinator that finalizes `transaction.version` 2 (KIP-890 part 2): the Produce v12 and every version
        // above it enrols the partition
        $client  = $this->client(2);
        $manager = new TransactionManager($client, 'orders-tx', 30000);
        $manager->initTransactions();
        $manager->beginTransaction();

        $client->produce([self::TOPIC => [0 => [new Record('protocol v2')]]], $manager);

        self::assertTrue($manager->isTransactionV2Enabled());
        self::assertSame([null], $client->produceVersionCaps, 'no cap');
        self::assertSame(13, Client::produceVersion(RecordBatch::MAGIC, $client->produceVersionCaps[0]));
    }

    private function client(?int $transactionVersion = null): FakeClient
    {
        $client                     = new FakeClient(
            ClusterFixture::withPartitions([self::TOPIC => [0 => 1]]),
            [ProducerConfig::ACKS => ProducerConfig::ACKS_ALL]
        );
        $client->producerIds        = [new ProducerIdAndEpoch(4000, 0)];
        $client->transactionVersion = $transactionVersion;

        return $client;
    }
}
