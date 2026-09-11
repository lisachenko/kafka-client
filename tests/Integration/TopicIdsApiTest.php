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
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\MetadataRequestTopic;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequestV10;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\MetadataResponseV10;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequest;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequestV3;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponse;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponseV3;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV5;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV5;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV8;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV8;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * What **Kafka 2.8** added to the four apis the producer and the consumer send, against the container.
 *
 * Three of the five versions are the **flexible** encoding of KIP-482 and nothing else - Produce **v9**,
 * ListOffsets **v6** and OffsetForLeaderEpoch **v4** - and the fourth and fifth are the two halves of the topic
 * ids: Metadata **v10** (KIP-516) puts the id of a topic into every entry of the request and of the answer, and
 * Metadata **v11** (KIP-700) takes `cluster_authorized_operations` out again.
 *
 * @see docs/protocol/2.8.md, sections "Topic ids (v10, KIP-516)", "Metadata API (key 3, v0 to v11)",
 *      "Produce API (key 0, v0 to v9)" and "Offsets API (key 2, v0 to v6), a.k.a. ListOffset"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(OffsetsRequest::class)]
#[CoversClass(OffsetsResponse::class)]
#[CoversClass(OffsetForLeaderEpochRequest::class)]
#[CoversClass(OffsetForLeaderEpochResponse::class)]
#[CoversClass(MetadataRequest::class)]
#[CoversClass(MetadataResponse::class)]
#[CoversClass(MetadataRequestTopic::class)]
#[CoversClass(TopicMetadata::class)]
#[CoversClass(Uuid::class)]
final class TopicIdsApiTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2-28';

    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * Topic of the current test, one partition on one broker
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t2-28-ids');
        self::createTopic($this->topic);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        // The property is only set once setUp() ran past its skip, i.e. only when there is a broker at all
        if (isset($this->topic)) {
            self::deleteTopic($this->topic);
        }

        parent::tearDown();
    }

    public function testAMetadataAnswerNamesTheTopicIdOfKip516(): void
    {
        $stream = $this->connect();
        new MetadataRequest([$this->topic], false, self::CLIENT_ID, 1000)->writeTo($stream);
        $topic = MetadataResponse::unpack($stream)->topics[$this->topic];

        self::assertSame(KafkaException::NO_ERROR, $topic->topicErrorCode);
        self::assertSame(Uuid::SIZE, strlen($topic->topicId), 'a uuid is 16 raw bytes, never compact');
        self::assertFalse(Uuid::isZero($topic->topicId), 'a topic of a 2.8 broker really has an id');
        self::assertSame(22, strlen(Uuid::toString($topic->topicId)), 'and it prints as 22 base64 characters');
        self::assertSame($topic->topicId, Uuid::fromString(Uuid::toString($topic->topicId)));
    }

    public function testTheTopicIdOfANewTopicOfTheSameNameIsAnotherOne(): void
    {
        // This is what KIP-516 is for: the id belongs to the topic, not to its name, so a broker that missed a
        // deletion can tell the two apart
        $first = $this->topicIdOf($this->topic, 1001);

        self::deleteTopic($this->topic);
        self::createTopic($this->topic);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);

        $second = $this->topicIdOf($this->topic, 1002);

        self::assertNotSame(
            Uuid::toString($first),
            Uuid::toString($second),
            'the re-created topic carries a new id under the very same name'
        );
    }

    public function testTheClusterWideBitfieldOfKip430IsGoneFromVersionEleven(): void
    {
        // The version 10 question, the last one that carries `include_cluster_authorized_operations`
        $stream = $this->connect();
        new MetadataRequestV10([$this->topic], false, self::CLIENT_ID, 1003, true, true)->writeTo($stream);
        $ten = MetadataResponseV10::unpack($stream);

        self::assertTrue(AclOperation::wasRequested($ten->clusterAuthorizedOperations));
        self::assertTrue(AclOperation::wasRequested($ten->topics[$this->topic]->authorizedOperations));

        // And the version 11 this client sends, which has no such field on the wire at all (KIP-700): asking for
        // it changes nothing, and the property keeps the Integer.MIN_VALUE of "you did not ask"
        $stream = $this->connect();
        new MetadataRequest([$this->topic], false, self::CLIENT_ID, 1004, true, true)->writeTo($stream);
        $eleven = MetadataResponse::unpack($stream);

        self::assertSame(AclOperation::NOT_REQUESTED, $eleven->clusterAuthorizedOperations);
        self::assertTrue(
            AclOperation::wasRequested($eleven->topics[$this->topic]->authorizedOperations),
            'the per-topic bitfield of KIP-430 is untouched'
        );
        self::assertSame(
            $ten->topics[$this->topic]->topicId,
            $eleven->topics[$this->topic]->topicId,
            'and both answers name the same topic id'
        );
    }

    public function testTheFlexibleVersionsOfKafkaTwoEightAreTheSameExchangesInFewerBytes(): void
    {
        $batch = RecordBatch::fromRecords([new Record('kafka 2.8')->withCreateTime(1600000000000)]);

        // Produce v9 against v8: the same body, written with compact types and a compact record set
        $stream = $this->connect();
        $flexibleProduce = new ProduceRequest(
            [$this->topic => [0 => $batch]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            1005
        );
        $flexibleProduce->writeTo($stream);
        $produced = ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[0];

        $stream = $this->connect();
        $plainProduce = new ProduceRequestV8(
            [$this->topic => [0 => $batch]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            1006
        );
        $plainProduce->writeTo($stream);
        $producedPlain = ProduceResponseV8::unpack($stream)->topics[$this->topic]->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $produced->errorCode);
        self::assertSame(KafkaException::NO_ERROR, $producedPlain->errorCode);
        self::assertSame(0, $produced->baseOffset, 'the flexible request appended the first record');
        self::assertSame(1, $producedPlain->baseOffset, 'and the plain one the second');
        self::assertLessThan(
            $plainProduce->getMessageSize(),
            $flexibleProduce->getMessageSize(),
            'the compact encoding is the shorter frame'
        );

        // ListOffsets v6 against v5
        $stream = $this->connect();
        new OffsetsRequest([$this->topic => [0 => OffsetsRequest::LATEST]], -1, 0, self::CLIENT_ID, 1007)
            ->writeTo($stream);
        $flexibleOffsets = OffsetsResponse::unpack($stream)->topics[$this->topic]->partitions[0];

        $stream = $this->connect();
        new OffsetsRequestV5([$this->topic => [0 => OffsetsRequest::LATEST]], -1, 0, self::CLIENT_ID, 1008)
            ->writeTo($stream);
        $plainOffsets = OffsetsResponseV5::unpack($stream)->topics[$this->topic]->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $flexibleOffsets->errorCode);
        self::assertSame(2, $flexibleOffsets->offset, 'both records are in the log');
        self::assertSame($plainOffsets->offset, $flexibleOffsets->offset);
        self::assertSame($plainOffsets->leaderEpoch, $flexibleOffsets->leaderEpoch);
        self::assertSame(
            FetchRequest::READ_UNCOMMITTED,
            0,
            'the isolation level of the request is the read_uncommitted of a plain consumer'
        );

        // OffsetForLeaderEpoch v4 against v3
        $epoch  = $plainOffsets->leaderEpoch;
        $stream = $this->connect();
        new OffsetForLeaderEpochRequest([$this->topic => [0 => [$epoch, $epoch]]], self::CLIENT_ID, 1009)
            ->writeTo($stream);
        $flexibleEpoch = OffsetForLeaderEpochResponse::unpack($stream)->topics[$this->topic]->partitions[0];

        $stream = $this->connect();
        new OffsetForLeaderEpochRequestV3([$this->topic => [0 => [$epoch, $epoch]]], self::CLIENT_ID, 1010)
            ->writeTo($stream);
        $plainEpoch = OffsetForLeaderEpochResponseV3::unpack($stream)->topics[$this->topic]->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $flexibleEpoch->errorCode);
        self::assertSame($plainEpoch->endOffset, $flexibleEpoch->endOffset);
        self::assertSame($plainEpoch->leaderEpoch, $flexibleEpoch->leaderEpoch);
        self::assertSame(2, $flexibleEpoch->endOffset, 'the epoch of this log ends at the end of the log');
    }

    /**
     * Returns the topic id a Metadata v11 answer names for the given topic
     */
    private function topicIdOf(string $topic, int $correlationId): string
    {
        $stream = $this->connect();
        new MetadataRequest([$topic], false, self::CLIENT_ID, $correlationId)->writeTo($stream);

        return MetadataResponse::unpack($stream)->topics[$topic]->topicId;
    }

    /**
     * Deletes a topic of this test through the `kafka-topics.sh` of the container
     */
    private static function deleteTopic(string $topic): void
    {
        $output   = [];
        $exitCode = 0;
        exec(
            sprintf(
                'docker exec %s /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete'
                . ' --topic %s 2>&1',
                escapeshellarg(self::container()),
                escapeshellarg($topic)
            ),
            $output,
            $exitCode
        );
    }

    /**
     * Creates a one-partition topic through the `kafka-topics.sh` of the container
     */
    private static function createTopic(string $topic): void
    {
        $output   = [];
        $exitCode = 0;
        exec(
            sprintf(
                'docker exec %s /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create'
                . ' --if-not-exists --topic %s --partitions 1 --replication-factor 1 2>&1',
                escapeshellarg(self::container()),
                escapeshellarg($topic)
            ),
            $output,
            $exitCode
        );

        if ($exitCode !== 0) {
            self::fail("Can not create the topic {$topic}: " . implode("\n", $output));
        }
    }

    /**
     * Name of the container the broker of this line runs in
     */
    private static function container(): string
    {
        $container = getenv('KAFKA_CONTAINER');

        return $container === false || trim($container) === '' ? 'kafka-2-8-2' : trim($container);
    }
}
