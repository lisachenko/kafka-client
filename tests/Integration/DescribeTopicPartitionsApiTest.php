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
use Protocol\Kafka\Admin\TopicDescription;
use Protocol\Kafka\Admin\TopicPartitionInfo;
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsCursor;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsResponseTopic;
use Protocol\Kafka\Protocol\Request\DescribeTopicPartitionsRequest;
use Protocol\Kafka\Protocol\Request\DescribeTopicPartitionsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * DescribeTopicPartitions (key 75, v0), the api Kafka 3.8 added with KIP-966, against the 3.9.2 KRaft node.
 *
 * It is the first api of this protocol that **pages**: the request bounds the answer with a
 * `response_partition_limit` and may start it at a `cursor`, and the answer hands the next cursor back. The
 * cursor is also the first *nullable structure* the protocol carries - one int8 in front of the structure - and
 * every partition of the answer holds the two ELR arrays of KIP-966, which this node writes empty.
 *
 * The node is shared, so every topic of this class carries the prefix `t1-38-dtp-` and is deleted again.
 *
 * @see docs/protocol/3.9.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeTopicPartitionsRequest::class)]
#[CoversClass(DescribeTopicPartitionsResponse::class)]
#[CoversClass(DescribeTopicPartitionsResponseTopic::class)]
#[CoversClass(DescribeTopicPartitionsResponsePartition::class)]
#[CoversClass(DescribeTopicPartitionsCursor::class)]
#[CoversClass(TopicDescription::class)]
#[CoversClass(TopicPartitionInfo::class)]
final class DescribeTopicPartitionsApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t1-38-dtp';

    /**
     * The one SASL user of the image that `super.users` does not name
     */
    private const string UNPRIVILEGED_USER = 'acltest';

    private const string UNPRIVILEGED_PASSWORD = 'acltest-secret';

    /**
     * How long to wait for a fresh topic, in seconds
     */
    private const float TIMEOUT = 30.0;

    private Cluster $cluster;

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
    }

    /**
     * Every partition of a topic, with the two ELR arrays of KIP-966 and the acl bit field nobody asked for
     */
    public function testTheApiDescribesEveryPartitionOfATopic(): void
    {
        $topic = $this->topic('three', 3);

        $described = $this->admin->describeTopicPartitions([$topic]);

        self::assertArrayHasKey($topic, $described);
        $description = $described[$topic];
        self::assertInstanceOf(TopicDescription::class, $description);
        self::assertSame($topic, $description->name);
        self::assertFalse($description->internal);
        self::assertNotSame(Uuid::ZERO, $description->topicId, 'the topic id of KIP-516 is part of the answer');
        self::assertSame([0, 1, 2], array_keys($description->partitions));
        self::assertNotSame(
            [],
            $description->authorizedOperations(),
            'the api reports the bit field of KIP-430 without a flag to ask for it'
        );

        foreach ($description->partitions as $index => $partition) {
            self::assertSame($index, $partition->partition);
            self::assertTrue($partition->hasLeader(), 'the one broker of the node leads every partition');
            self::assertSame([$partition->leader], $partition->replicas);
            self::assertSame([$partition->leader], $partition->isr);
            self::assertSame([], $partition->offlineReplicas);
            self::assertSame(
                [],
                $partition->elr,
                'the node writes the eligible leader replicas as an EMPTY array, not as the null of the spec'
            );
            self::assertSame([], $partition->lastKnownElr);
        }
    }

    /**
     * The client walks the pages, so a limit of one partition answers the same description as no limit at all
     */
    public function testTheClientPagesUntilTheListingIsComplete(): void
    {
        $topic = $this->topic('paged', 3);

        $whole = $this->admin->describeTopicPartitions([$topic]);
        $paged = $this->admin->describeTopicPartitions([$topic], 1);

        self::assertInstanceOf(TopicDescription::class, $whole[$topic]);
        self::assertInstanceOf(TopicDescription::class, $paged[$topic]);
        self::assertSame(array_keys($whole[$topic]->partitions), array_keys($paged[$topic]->partitions));
        self::assertEquals($whole[$topic]->partitions, $paged[$topic]->partitions);
    }

    /**
     * The raw frames of the walk: what the cursor of an answer names is the first partition that is MISSING
     */
    public function testTheNextCursorNamesTheFirstPartitionThatIsMissing(): void
    {
        $topic  = $this->topic('cursor', 3);
        $stream = $this->connect();

        $first = $this->exchange($stream, new DescribeTopicPartitionsRequest([$topic], 1, null, self::CLIENT_ID, 1));

        self::assertSame([0], array_keys($first->topics[$topic]->partitions));
        self::assertNotNull($first->nextCursor);
        self::assertSame($topic, $first->nextCursor->topicName);
        self::assertSame(1, $first->nextCursor->partitionIndex);

        $second = $this->exchange(
            $stream,
            new DescribeTopicPartitionsRequest([$topic], 1, $first->nextCursor, self::CLIENT_ID, 2)
        );

        self::assertSame([1], array_keys($second->topics[$topic]->partitions));
        self::assertSame(2, $second->nextCursor?->partitionIndex);

        $third = $this->exchange(
            $stream,
            new DescribeTopicPartitionsRequest([$topic], 1, $second->nextCursor, self::CLIENT_ID, 3)
        );

        self::assertSame([2], array_keys($third->topics[$topic]->partitions));
        self::assertNull($third->nextCursor, 'the page that ends the listing carries no cursor');
    }

    /**
     * The answer is sorted by topic name, whatever order the request named the topics in
     */
    public function testTheAnswerIsSortedByTopicName(): void
    {
        $first  = $this->topic('a-sorted', 1);
        $second = $this->topic('b-sorted', 1);
        $sorted = [$first, $second];
        sort($sorted);

        $answer = $this->exchange(
            $this->connect(),
            new DescribeTopicPartitionsRequest([$second, $first], 2000, null, self::CLIENT_ID, 4)
        );

        self::assertSame($sorted, array_keys($answer->topics), 'the order of the request is lost');
    }

    /**
     * A topic the cluster does not host is the code 3 in its own entry, never an exception of the call
     */
    public function testAnUnknownTopicIsTheCode3InItsOwnEntry(): void
    {
        $known   = $this->topic('known', 1);
        $unknown = self::uniqueTopicName('t1-38-dtp-absent');

        $described = $this->admin->describeTopicPartitions([$known, $unknown]);

        self::assertInstanceOf(TopicDescription::class, $described[$known]);
        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $described[$unknown]);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $described[$unknown]->getCode());
    }

    /**
     * A name that is not a legal topic name is the 17, and the other topics of the frame are still answered
     */
    public function testAnIllegalTopicNameIsTheCode17(): void
    {
        $described = $this->admin->describeTopicPartitions(['t1-38-dtp not a legal name']);

        self::assertInstanceOf(InvalidTopicException::class, $described['t1-38-dtp not a legal name']);
    }

    /**
     * `__consumer_offsets` is answered with the `is_internal` flag the ordinary topics of a test do not carry
     */
    public function testAnInternalTopicIsMarkedInternal(): void
    {
        $described = $this->admin->describeTopicPartitions(['__consumer_offsets']);

        $description = $described['__consumer_offsets'];
        self::assertInstanceOf(TopicDescription::class, $description);
        self::assertTrue($description->internal, 'the log of the group coordinator belongs to Kafka itself');
        self::assertNotSame([], $description->partitions);
    }

    /**
     * An empty topic list is "every topic of the cluster", which the shared node answers as a superset
     */
    public function testAnEmptyTopicListDescribesEveryTopicOfTheCluster(): void
    {
        $topic = $this->topic('every', 1);

        $described = $this->admin->describeTopicPartitions([]);

        self::assertArrayHasKey($topic, $described, 'the empty array is every topic, not no topic');
        self::assertArrayHasKey('__consumer_offsets', $described, 'the internal topics are part of it');
        self::assertInstanceOf(TopicDescription::class, $described[$topic]);
    }

    /**
     * A cursor the node refuses is the 42 in the entry of every topic the request named
     */
    public function testACursorTheRequestDoesNotNameIsThe42(): void
    {
        $topic = $this->topic('refused-cursor', 1);
        $other = self::uniqueTopicName('t1-38-dtp-elsewhere');

        $answer = $this->exchange(
            $this->connect(),
            new DescribeTopicPartitionsRequest(
                [$topic],
                2000,
                new DescribeTopicPartitionsCursor($other, 0),
                self::CLIENT_ID,
                5
            )
        );

        self::assertSame(KafkaException::INVALID_REQUEST, $answer->topics[$topic]->errorCode);
        self::assertSame([], $answer->topics[$topic]->partitions);
        self::assertSame(Uuid::ZERO, $answer->topics[$topic]->topicId);
        self::assertSame(
            AclOperation::NOT_REQUESTED,
            $answer->topics[$topic]->topicAuthorizedOperations,
            'the error answer of this api carries no bit field'
        );

        $negative = $this->exchange(
            $this->connect(),
            new DescribeTopicPartitionsRequest(
                [$topic],
                2000,
                new DescribeTopicPartitionsCursor($topic, -1),
                self::CLIENT_ID,
                6
            )
        );

        self::assertSame(
            KafkaException::INVALID_REQUEST,
            $negative->topics[$topic]->errorCode,
            'a negative partition index is the second cursor the node refuses'
        );
    }

    /**
     * A cursor that points past the end of its topic is legal, and it is answered with no partition at all
     */
    public function testACursorPastTheEndIsAnEmptyPartitionArray(): void
    {
        $topic = $this->topic('past-the-end', 1);

        $answer = $this->exchange(
            $this->connect(),
            new DescribeTopicPartitionsRequest(
                [$topic],
                2000,
                new DescribeTopicPartitionsCursor($topic, 99),
                self::CLIENT_ID,
                7
            )
        );

        self::assertSame(KafkaException::NO_ERROR, $answer->topics[$topic]->errorCode);
        self::assertSame([], $answer->topics[$topic]->partitions);
        self::assertNull($answer->nextCursor);
    }

    /**
     * A principal that may not describe a topic is answered 29 for the topic it named, and nothing for the rest
     */
    public function testAPrincipalWithoutRightsIsRefusedPerTopicAndAnsweredNothingForTheCluster(): void
    {
        $topic = $this->topic('refused', 1);

        $described = $this->unprivileged()->describeTopicPartitions([$topic]);

        self::assertInstanceOf(TopicAuthorizationFailedException::class, $described[$topic]);
        self::assertSame(KafkaException::TOPIC_AUTHORIZATION_FAILED, $described[$topic]->getCode());

        self::assertSame(
            [],
            $this->unprivileged()->describeTopicPartitions([]),
            'the same principal asking for every topic is answered an empty list, not a refusal'
        );
    }

    /**
     * Creates a topic of this class and waits until the node serves it
     */
    private function topic(string $purpose, int $partitions): string
    {
        $topic = self::uniqueTopicName("t1-38-dtp-{$purpose}");

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, $partitions, 1)]));

        // A fresh topic answers 3, 5 or 6 for a moment, until its leader is elected and known to this client
        $deadline = microtime(true) + self::TIMEOUT;
        do {
            try {
                $this->cluster->reload();
                $this->admin->listOffsets([$topic => range(0, $partitions - 1)], OffsetsRequest::LATEST);

                return $topic;
            } catch (KafkaException | TopicPartitionRequestException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    /**
     * Sends one frame and reads the answer back
     */
    private function exchange(
        SocketStream $stream,
        DescribeTopicPartitionsRequest $request
    ): DescribeTopicPartitionsResponse {
        $request->writeTo($stream);

        return DescribeTopicPartitionsResponse::unpack($stream);
    }

    /**
     * An admin client of the one SASL user the authorizer of the node has no acl for
     */
    private function unprivileged(): AdminClient
    {
        if (self::saslBootstrapServer() === '') {
            self::markTestSkipped(self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set, the refusal needs a principal');
        }

        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::saslBootstrapServer()],
            ClientConfig::SECURITY_PROTOCOL  => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM     => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME      => self::UNPRIVILEGED_USER,
            ClientConfig::SASL_PASSWORD      => self::UNPRIVILEGED_PASSWORD,
        ] + $this->configuration();

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * Client configuration pointing at the broker under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
