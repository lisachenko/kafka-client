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
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV17;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV17;

/**
 * Fetch v18 (Kafka 4.1, KIP-1166) against the node: the tagged high watermark a follower knows of a partition
 *
 * `FetchRequest.json` @ 4.1.0 declares `HighWatermark` as the tag 1 of a partition entry with the default
 * Long.MAX_VALUE ("the feature is not supported"), and `FetchResponse.json` "Version 18 no changes to the response
 * (KIP-1166)". The field is read by the raft client of the metadata log on the controller listener; a fetch of an
 * ordinary topic is answered the same with and without it.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)" and "The high watermark of a follower, KIP-1166
 *      (v18)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchRequestV17::class)]
#[CoversClass(FetchResponseV17::class)]
#[CoversClass(FetchRequestTopicPartition::class)]
#[CoversClass(Client::class)]
final class FetchHighWatermarkTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t2-41-hw';

    private const int TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    private const array VALUES = ['one', 'two'];

    private Client $client;

    private AdminClient $admin;

    private string $topic;

    private string $topicId;

    protected function setUp(): void
    {
        parent::setUp();

        $cluster      = Cluster::bootstrap($this->configuration());
        $this->client = new Client($cluster, $this->configuration());
        $this->admin  = new AdminClient($cluster, $this->configuration());
        $this->topic  = self::uniqueTopicName('t2-41-hw');

        self::assertSame([$this->topic => null], $this->admin->createTopics([new NewTopic($this->topic, 1, 1)]));
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        while (true) {
            try {
                $this->client->produce([$this->topic => [0 => array_map(
                    static fn(string $value): Record => new Record($value),
                    self::VALUES
                )]]);
                break;
            } catch (KafkaException $exception) {
                if (microtime(true) > $deadline) {
                    throw $exception;
                }
                usleep(200000);
                $cluster->reload();
            }
        }
        $this->topicId = self::topicIdOf($this->topic);
    }

    protected function tearDown(): void
    {
        if (isset($this->topic)) {
            try {
                $this->admin->deleteTopics([$this->topic]);
            } catch (KafkaException) {
                // A node that can not delete the topic right now must not fail the test that just passed
            }
        }

        parent::tearDown();
    }

    public function testAConsumerFrameOfVersionEighteenIsTheVersionSeventeenFrameAndIsAnsweredTheSame(): void
    {
        $stream    = $this->connect();
        $arguments = $this->arguments([0 => [0, 0]], 4201);

        $seventeen = new FetchRequestV17(...$arguments);
        $eighteen  = new FetchRequest(...$arguments);
        self::assertSame(
            bin2hex((string) $seventeen),
            substr_replace(bin2hex((string) $eighteen), '0011', 12, 4),
            'a consumer states no high watermark, so only the api version differs'
        );

        $old = $this->send($stream, $seventeen, FetchResponseV17::class);
        $new = $this->send($stream, $eighteen, FetchResponse::class);

        self::assertSame(18, $new::VERSION);
        self::assertSame(bin2hex((string) $old), bin2hex((string) $new), 'the answer of version 18 is the one of 17');
        $partition = self::fetchedTopic($new, $this->topic)->partitions[0];
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(count(self::VALUES), $partition->highWaterMarkOffset);
    }

    public function testTheHighWatermarkOfAConsumerChangesNothing(): void
    {
        $stream = $this->connect();
        $plain  = $this->send($stream, new FetchRequest(...$this->arguments([0 => [0, 0]], 4210)), FetchResponse::class);

        foreach ([1, FetchRequestTopicPartition::UNKNOWN_HIGH_WATERMARK, 1000] as $highWatermark) {
            $request = new FetchRequest(...$this->arguments([0 => [0, 0, -1, $highWatermark]], 4210));
            self::assertStringContainsString(
                '010108' . sprintf('%016x', $highWatermark),
                bin2hex((string) $request),
                'the tag 1, its size 8 and the value end the partition entry'
            );

            $answer = $this->send($stream, $request, FetchResponse::class);
            self::assertSame(bin2hex((string) $plain), bin2hex((string) $answer), "high watermark {$highWatermark}");
        }
    }

    public function testAFollowerClaimWithTheHighWatermarkIsAnsweredAsOneWithout(): void
    {
        $stream = $this->connect();

        // The replica state 1 / 0 of KIP-903 without a current leader epoch: the one broker leads the partition and
        // is not a follower of it, so `Partition.followerReplicaOrThrow` refuses the claim with the 6
        $without = $this->send(
            $stream,
            new FetchRequest(...$this->arguments([0 => [0, -1]], 4220, 1, 0)),
            FetchResponse::class
        );
        $with = $this->send(
            $stream,
            new FetchRequest(...$this->arguments([0 => [0, -1, -1, 1]], 4220, 1, 0)),
            FetchResponse::class
        );

        self::assertSame(bin2hex((string) $without), bin2hex((string) $with));
        $partition = self::fetchedTopic($with, $this->topic)->partitions[0];
        self::assertSame(KafkaException::NOT_LEADER_FOR_PARTITION, $partition->errorCode);
        self::assertSame(1, $partition->currentLeader?->leaderId);
        self::assertSame([], $with->nodeEndpoints, 'a node of Kafka 4.0 or later tells a follower no endpoint');

        // With the current leader epoch 0 the claim is the 75 of KIP-320, again with and without the tag
        foreach ([[0, 0], [0, 0, -1, 1]] as $partitionValue) {
            $answer = $this->send(
                $stream,
                new FetchRequest(...$this->arguments([0 => $partitionValue], 4221, 1, 0)),
                FetchResponse::class
            );
            self::assertSame(
                KafkaException::UNKNOWN_LEADER_EPOCH,
                self::fetchedTopic($answer, $this->topic)->partitions[0]->errorCode
            );
        }
    }

    public function testTheClientFetchesAtVersionEighteen(): void
    {
        self::assertSame(18, FetchRequest::VERSION);

        $fetched = $this->client->fetchPartitions([$this->topic => [0 => 0]], 1000);

        self::assertSame(
            self::VALUES,
            array_map(static fn(Record $record): ?string => $record->value, $fetched[$this->topic][0]->getRecords())
        );
    }

    /**
     * @param array<int, int|list<int>> $partitions
     *
     * @return list<mixed> Arguments of a session-less fetch of the topic of this test
     */
    private function arguments(array $partitions, int $correlationId, int $replicaId = -1, ?int $replicaEpoch = null): array
    {
        return [
            [$this->topic => $partitions],
            250,
            1,
            1048576,
            $replicaId,
            self::CLIENT_ID,
            $correlationId,
            52428800,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            [$this->topic => $this->topicId],
            $replicaEpoch,
        ];
    }

    /**
     * @param class-string<FetchResponse> $responseClass
     */
    private function send(SocketStream $stream, FetchRequest $request, string $responseClass): FetchResponse
    {
        $request->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    /**
     * @return array<string, mixed> Client configuration of this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => self::TIMEOUT_MS,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::TIMEOUT_MS,

            ProducerConfig::ACKS                    => ProducerConfig::ACKS_LEADER,
            ProducerConfig::TIMEOUT_MS              => self::TIMEOUT_MS,

            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
        ] + ConsumerConfig::getDefaultConfiguration() + ProducerConfig::getDefaultConfiguration();
    }
}
