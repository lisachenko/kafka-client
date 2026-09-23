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
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\ClusterFixture;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\FakeClient;

/**
 * The version choice of Produce (Kafka 4.0): v12 outside a transaction, v11 inside one, v2 for a message set.
 *
 * `ProduceRequest.json` @ 4.0.0 on version 12: "Note when produce requests are used in transaction, if transaction V2
 * (KIP_890 part 2) is enabled, the produce request will also include the function for a AddPartitionsToTxn call. If
 * V2 is disabled, the client can't use produce request version higher than 11 within a transaction." The choice lives
 * in {@see Client::produceVersion()}, and the cap of a transactional producer is handed to it by
 * {@see Client::produce()}.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v12)" and "The transaction protocol v2 of KIP-890
 *      part 2 (v12)"
 */
#[CoversClass(Client::class)]
final class ProduceVersionChoiceTest extends TestCase
{
    private const string TOPIC = 'orders';

    public function testTheRecordBatchGoesOutAsVersionTwelve(): void
    {
        self::assertSame(12, Client::produceVersion(RecordBatch::MAGIC));
        self::assertSame(ProduceRequest::VERSION, Client::produceVersion(RecordBatch::MAGIC, null));
    }

    public function testACapLowersTheVersionOfTheRecordBatch(): void
    {
        self::assertSame(11, Client::produceVersion(RecordBatch::MAGIC, ProduceRequestV11::VERSION));
        self::assertSame(12, Client::produceVersion(RecordBatch::MAGIC, 99), 'a cap above the table changes nothing');
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

    public function testATransactionalProducerIsCappedAtVersionEleven(): void
    {
        $client  = $this->client();
        $manager = new TransactionManager($client, 'orders-tx', 30000);

        $client->produce([self::TOPIC => [0 => [new Record('in a transaction')]]], $manager);

        self::assertSame(
            [ProduceRequestV11::VERSION],
            $client->produceVersionCaps,
            'a v12 inside a transaction is the transaction protocol v2, which a producer of the v1 must not send'
        );
    }

    private function client(): FakeClient
    {
        $client              = new FakeClient(
            ClusterFixture::withPartitions([self::TOPIC => [0 => 1]]),
            [ProducerConfig::ACKS => ProducerConfig::ACKS_ALL]
        );
        $client->producerIds = [new ProducerIdAndEpoch(4000, 0)];

        return $client;
    }
}
