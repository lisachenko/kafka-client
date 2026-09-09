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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\ClientQuota;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the throttle time of Produce v1 and Fetch v1 against a real Kafka 0.9.0.1 broker with client quotas.
 *
 * Quotas are the only thing that makes a 0.9 broker report a throttle time, and there is no api to configure them:
 * they are written into ZooKeeper with the `kafka-configs.sh` tool of the distribution, which {@see ClientQuota}
 * runs inside the broker container. These tests are therefore skipped when Docker is not available, exactly like
 * the whole suite is skipped without a broker.
 *
 * Every test uses a client id of its own - the broker enforces a quota for whoever sends that id, and this broker
 * is shared with the other suites - and removes its quota in a `finally` block, also when it fails.
 *
 * @see docs/protocol/0.10.2.md, section "Quotas and throttle time"
 */
#[CoversClass(Client::class)]
#[CoversClass(KafkaProducer::class)]
#[CoversClass(RecordMetadata::class)]
#[CoversClass(FetchedPartition::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(FetchResponse::class)]
final class QuotaThrottleTest extends IntegrationTestCase
{
    /**
     * Quota that the tests set for their own client id, in bytes per second
     *
     * The broker measures the rate over `quota.window.num` samples of `quota.window.size.seconds` - 11 seconds by
     * default - so a client that stays just above this bound is delayed by about a second, and the tests stay fast.
     */
    private const int BYTE_RATE = 1024;

    /**
     * Size of the value of a single record, in bytes
     */
    private const int RECORD_SIZE = 1024;

    /**
     * How many requests a test sends at most while it waits for the broker to start throttling
     */
    private const int MAX_ATTEMPTS = 30;

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * How long a fetch waits for its answer, in milliseconds
     *
     * This has to be longer than the delay a throttled answer is held back for: the client waits for the answer of
     * a fetch at most twice as long as the `fetch.max.wait.ms` it sent, so a consumer with a short wait time runs
     * into its own timeout instead of reading the throttled answer.
     */
    private const int FETCH_WAIT_MS = 10000;

    /**
     * How long to wait for an auto-created topic to become writable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * Topic of the current test
     */
    private string $topic;

    /**
     * Client id of the current test, unique so that the quota only applies to it
     */
    private string $clientId;

    /**
     * Quota of that client id, removed again by {@see self::tearDown()}
     */
    private ?ClientQuota $quota = null;

    protected function setUp(): void
    {
        parent::setUp();

        if (!ClientQuota::isSupported()) {
            $this->markTestSkipped(
                'The quotas of the broker can not be configured from here: the container '
                . '(' . ClientQuota::CONTAINER_ENV . ') is not reachable with `docker exec`'
            );
        }

        $this->clientId = 't8-quota-' . bin2hex(random_bytes(4));
        $this->topic    = self::uniqueTopicName('t8-quota');
        $this->quota    = ClientQuota::forClientId($this->clientId);

        new TopicMetadataProbe(fn(): Stream => $this->connect(), self::TOPIC_TIMEOUT, $this->clientId)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        // A quota that is left behind would slow this client id down for an hour, and the broker is shared
        $this->quota?->remove();
        $this->quota = null;
    }

    public function testAProducerQuotaDelaysTheAnswerAndTheProducerReportsIt(): void
    {
        $quota = $this->quota;
        self::assertInstanceOf(ClientQuota::class, $quota);
        $quota->set(['producer_byte_rate' => self::BYTE_RATE]);

        try {
            $producer  = new KafkaProducer($this->configuration());
            $throttled = null;
            $elapsedMs = 0.0;

            // `batch.size` defaults to 0, so every record is one request and every promise is settled by send()
            for ($attempt = 0; $attempt < self::MAX_ATTEMPTS && $throttled === null; $attempt++) {
                $metadata = null;
                $started  = microtime(true);
                $producer
                    ->send($this->topic, Record::fromValue(str_repeat('t8', self::RECORD_SIZE / 2)), 0)
                    ->then(static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                        $metadata = $recordMetadata;
                    });
                $roundTripMs = (microtime(true) - $started) * 1000;

                self::assertInstanceOf(RecordMetadata::class, $metadata, 'the batch was not acknowledged');
                self::assertSame($attempt, $metadata->offset, 'a throttled batch is appended like any other');

                if ($metadata->throttleTimeMs > 0) {
                    $throttled = $metadata;
                    $elapsedMs = $roundTripMs;
                }
            }

            self::assertInstanceOf(
                RecordMetadata::class,
                $throttled,
                sprintf(
                    'The broker did not throttle %d records of %d bytes with %s',
                    self::MAX_ATTEMPTS,
                    self::RECORD_SIZE,
                    $quota->describe()
                )
            );
            self::assertGreaterThan(0, $throttled->throttleTimeMs);
            self::assertNotNull($throttled->timestamp, 'the CreateTime the producer stamped on the batch');

            // The broker throttles by holding the answer back, so the round trip took at least that long
            self::assertGreaterThanOrEqual(
                $throttled->throttleTimeMs * 0.9,
                $elapsedMs,
                'the answer of a throttled batch arrived earlier than the delay it reports'
            );
        } finally {
            $quota->remove();
        }
    }

    public function testAConsumerQuotaDelaysTheFetchAndEveryPartitionOfItCarriesTheThrottleTime(): void
    {
        $quota = $this->quota;
        self::assertInstanceOf(ClientQuota::class, $quota);

        $configuration = $this->configuration();
        $cluster       = Cluster::bootstrap($configuration, $this->topic);
        $client        = new Client($cluster, $configuration);

        // The records are written before the quota exists, so only the fetches below are throttled
        $records = [];
        for ($index = 0; $index < 8; $index++) {
            $records[] = Record::fromValue(str_repeat('t8', self::RECORD_SIZE / 2));
        }
        $client->produce([$this->topic => [0 => $records]]);

        $quota->set(['consumer_byte_rate' => self::BYTE_RATE]);

        try {
            $throttled = null;
            $elapsedMs = 0.0;
            for ($attempt = 0; $attempt < self::MAX_ATTEMPTS && $throttled === null; $attempt++) {
                $started   = microtime(true);
                $partition = $client->fetchPartitions(
                    [$this->topic => [0 => 0]],
                    self::FETCH_WAIT_MS
                )[$this->topic][0];
                $roundTripMs = (microtime(true) - $started) * 1000;

                self::assertInstanceOf(FetchedPartition::class, $partition);
                self::assertSame(0, $partition->errorCode, 'a quota is not an error, the answer is only delayed');
                self::assertNotSame([], $partition->getRecords(), 'a throttled fetch still returns its records');

                if ($partition->throttleTimeMs > 0) {
                    $throttled = $partition;
                    $elapsedMs = $roundTripMs;
                }
            }

            self::assertInstanceOf(
                FetchedPartition::class,
                $throttled,
                sprintf(
                    'The broker did not throttle %d fetches of the topic %s with %s',
                    self::MAX_ATTEMPTS,
                    $this->topic,
                    $quota->describe()
                )
            );
            self::assertGreaterThan(0, $throttled->throttleTimeMs);
            self::assertSame(count($records), $throttled->highWaterMarkOffset);
            self::assertGreaterThanOrEqual(
                $throttled->throttleTimeMs * 0.9,
                $elapsedMs,
                'the throttled answer arrived earlier than the delay it reports'
            );
        } finally {
            $quota->remove();
        }
    }

    /**
     * Configuration of a client that sends the unique client id of this test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID          => $this->clientId,
            // A throttled answer arrives late by definition, so the client has to be willing to wait for it
            ClientConfig::REQUEST_TIMEOUT_MS => 30000,

            ProducerConfig::ACKS       => 1,
            ProducerConfig::TIMEOUT_MS => self::PRODUCE_TIMEOUT_MS,

            ConsumerConfig::FETCH_MAX_WAIT_MS         => self::FETCH_WAIT_MS,
            ConsumerConfig::FETCH_MIN_BYTES           => 1,
            // Small fetches keep the byte rate just above the quota, so the delays stay in the range of a second
            ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 2 * self::RECORD_SIZE,
        ] + ProducerConfig::getDefaultConfiguration();
    }
}
