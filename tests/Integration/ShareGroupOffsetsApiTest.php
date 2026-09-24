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
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AlterShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\AlterShareGroupOffsetsResponse;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupOffsetsResponse;
use Protocol\Kafka\Protocol\Request\DescribeShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\DescribeShareGroupOffsetsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * The share-group offset apis 90 to 92 of Kafka 4.1 (KIP-932) against the 4.3.1 node, which finalizes
 * `share.version` 1.
 *
 * The node creates a share group with the first AlterShareGroupOffsets of it, which is how every test here gets a
 * group that exists and holds state without a share consumer of its own; the one test that needs a group with a
 * member runs the `kafka-console-share-consumer.sh` of the image for a bounded while. Every topic and every group
 * this class creates is deleted again - a share group with DeleteGroups, the api of every group type.
 *
 * These are wire classes: the admin methods over them belong to the share-consumer wave.
 *
 * @see docs/protocol/4.3.md, sections "DescribeShareGroupOffsets API (key 90, v0)", "AlterShareGroupOffsets API (key
 *      91, v0)" and "DeleteShareGroupOffsets API (key 92, v0)"
 */
#[CoversClass(DescribeShareGroupOffsetsRequest::class)]
#[CoversClass(DescribeShareGroupOffsetsResponse::class)]
#[CoversClass(AlterShareGroupOffsetsRequest::class)]
#[CoversClass(AlterShareGroupOffsetsResponse::class)]
#[CoversClass(DeleteShareGroupOffsetsRequest::class)]
#[CoversClass(DeleteShareGroupOffsetsResponse::class)]
final class ShareGroupOffsetsApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t4-41';

    private const int NON_EMPTY_GROUP = 68;

    private const int GROUP_ID_NOT_FOUND = 69;

    private const float TIMEOUT = 30.0;

    private Cluster $cluster;

    private AdminClient $admin;

    private Client $client;

    /**
     * @var list<string>
     */
    private array $createdTopics = [];

    /**
     * @var list<string>
     */
    private array $createdGroups = [];

    private int $correlationId = 4200;

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
        $this->client  = new Client($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        foreach ($this->createdGroups as $group) {
            try {
                $this->admin->deleteConsumerGroups([$group]);
            } catch (KafkaException) {
                // a group that was never created, or that the node deleted already
            }
        }
        $this->createdGroups = [];

        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testAnAlterCreatesTheShareGroupAndTheDescribeReadsItsStartOffsets(): void
    {
        $topic = $this->topic('alter', 2);
        $group = $this->group('alter');

        $altered = $this->exchange(
            new AlterShareGroupOffsetsRequest($group, [$topic => [0 => 3, 1 => 0, 2 => 0]], self::CLIENT_ID, $this->correlationId++),
            AlterShareGroupOffsetsResponse::class
        );

        self::assertSame(KafkaException::NO_ERROR, $altered->errorCode, 'the group is created by the alter');
        $partitions = $altered->responses[$topic]->partitions;
        self::assertSame(KafkaException::NO_ERROR, $partitions[0]->errorCode);
        self::assertSame(KafkaException::NO_ERROR, $partitions[1]->errorCode);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $partitions[2]->errorCode, 'no partition 2');
        self::assertNotSame(Uuid::ZERO, $altered->responses[$topic]->topicId);

        $described = $this->startOffsetsUntil($group, static fn(array $offsets): bool => $offsets === [0 => 3, 1 => 0], $topic);

        self::assertSame([0 => 3, 1 => 0], $described, 'every partition the group holds state for, with a null topic array');
    }

    public function testAbsentStateIsTheMinusOneAndNotAnError(): void
    {
        $topic   = $this->topic('absent', 1);
        $nobody  = $this->group('absent');
        $missing = self::uniqueTopicName('t4-41-share-missing');

        $answer = $this->exchange(
            new DescribeShareGroupOffsetsRequest([$nobody => [$topic => [0], $missing => [0]]], self::CLIENT_ID, $this->correlationId++),
            DescribeShareGroupOffsetsResponse::class
        );

        $group = $answer->groups[$nobody];
        self::assertSame(KafkaException::NO_ERROR, $group->errorCode, 'no 69 for a group that does not exist');
        self::assertSame(-1, $group->topics[$topic]->partitions[0]->startOffset);
        self::assertSame(-1, $group->topics[$missing]->partitions[0]->startOffset);
        self::assertSame(Uuid::ZERO, $group->topics[$missing]->topicId, 'a topic the node does not have');

        $all = $this->exchange(
            new DescribeShareGroupOffsetsRequest([$nobody => null], self::CLIENT_ID, $this->correlationId++),
            DescribeShareGroupOffsetsResponse::class
        );
        self::assertSame([], $all->groups[$nobody]->topics);
        self::assertSame(KafkaException::NO_ERROR, $all->groups[$nobody]->errorCode);
    }

    public function testTheDeleteForgetsATopicAndDoesNotCreateAGroup(): void
    {
        $topic   = $this->topic('delete', 1);
        $group   = $this->group('delete');
        $nobody  = $this->group('delete-nobody');
        $missing = self::uniqueTopicName('t4-41-share-missing');

        $this->exchange(
            new AlterShareGroupOffsetsRequest($group, [$topic => [0 => 0]], self::CLIENT_ID, $this->correlationId++),
            AlterShareGroupOffsetsResponse::class
        );
        $this->startOffsetsUntil($group, static fn(array $offsets): bool => $offsets === [0 => 0], $topic);

        $deleted = $this->exchange(
            new DeleteShareGroupOffsetsRequest($group, [$topic, $missing], self::CLIENT_ID, $this->correlationId++),
            DeleteShareGroupOffsetsResponse::class
        );

        self::assertSame(KafkaException::NO_ERROR, $deleted->errorCode);
        self::assertSame(KafkaException::NO_ERROR, $deleted->responses[$topic]->errorCode);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $deleted->responses[$missing]->errorCode);
        self::assertSame(
            [],
            $this->startOffsetsUntil($group, static fn(array $offsets): bool => $offsets === [], $topic),
            'the group holds no state of the topic any more'
        );

        $unknown = $this->exchange(
            new DeleteShareGroupOffsetsRequest($nobody, [$topic], self::CLIENT_ID, $this->correlationId++),
            DeleteShareGroupOffsetsResponse::class
        );

        self::assertSame(self::GROUP_ID_NOT_FOUND, $unknown->errorCode);
        self::assertSame("Group {$nobody} not found.", $unknown->errorMessage);
        self::assertSame([], $unknown->responses);
    }

    public function testAClassicGroupIsNotAShareGroup(): void
    {
        $topic   = $this->topic('classic', 1);
        $classic = $this->group('classic');
        $this->client->commitGroupOffsets($this->client->getGroupCoordinator($classic), $classic, '', -1, [$topic => [0 => 1]], -1);

        $altered = $this->exchange(
            new AlterShareGroupOffsetsRequest($classic, [$topic => [0 => 0]], self::CLIENT_ID, $this->correlationId++),
            AlterShareGroupOffsetsResponse::class
        );
        $deleted = $this->exchange(
            new DeleteShareGroupOffsetsRequest($classic, [$topic], self::CLIENT_ID, $this->correlationId++),
            DeleteShareGroupOffsetsResponse::class
        );

        foreach ([$altered, $deleted] as $answer) {
            self::assertSame(self::GROUP_ID_NOT_FOUND, $answer->errorCode);
            self::assertSame("Group {$classic} is not a share group.", $answer->errorMessage);
        }
    }

    public function testAGroupWithAMemberIsRefusedWithNonEmptyGroup(): void
    {
        $topic = $this->topic('member', 1);
        $group = $this->group('member');

        exec(sprintf(
            'docker exec -d %s timeout 20 /opt/kafka/bin/kafka-console-share-consumer.sh'
            . ' --bootstrap-server localhost:9092 --topic %s --group %s 2>&1',
            escapeshellarg(self::container()),
            escapeshellarg($topic),
            escapeshellarg($group)
        ));

        try {
            $this->awaitMembers($group, true);

            $altered = $this->exchange(
                new AlterShareGroupOffsetsRequest($group, [$topic => [0 => 0]], self::CLIENT_ID, $this->correlationId++),
                AlterShareGroupOffsetsResponse::class
            );
            $deleted = $this->exchange(
                new DeleteShareGroupOffsetsRequest($group, [$topic], self::CLIENT_ID, $this->correlationId++),
                DeleteShareGroupOffsetsResponse::class
            );

            foreach ([$altered, $deleted] as $answer) {
                self::assertSame(self::NON_EMPTY_GROUP, $answer->errorCode);
                self::assertSame('The group is not empty.', $answer->errorMessage);
                self::assertSame([], $answer->responses);
            }
        } finally {
            // The member leaves when its `timeout` ends; only an empty group can be deleted
            $this->awaitMembers($group, false);
        }
    }

    /**
     * Describes the group with a null topic array until its start offsets in the topic satisfy the predicate
     *
     * @param callable(array<int, int>): bool $isFinal
     *
     * @return array<int, int> Start offset of every partition of the topic the group holds state for
     */
    private function startOffsetsUntil(string $group, callable $isFinal, string $topic): array
    {
        $deadline = microtime(true) + self::TIMEOUT;
        do {
            $answer  = $this->exchange(
                new DescribeShareGroupOffsetsRequest([$group => null], self::CLIENT_ID, $this->correlationId++),
                DescribeShareGroupOffsetsResponse::class
            );
            $offsets = [];
            foreach ($answer->groups[$group]->topics[$topic]->partitions ?? [] as $partition => $result) {
                $offsets[$partition] = $result->startOffset;
            }
            ksort($offsets);
            if ($isFinal($offsets)) {
                return $offsets;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        return $offsets;
    }

    /**
     * Waits until the share group has members, or until it has none any more
     */
    private function awaitMembers(string $group, bool $present): void
    {
        $deadline = microtime(true) + 60.0;
        do {
            $output = (string) shell_exec(sprintf(
                'docker exec %s /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server localhost:9092'
                . ' --describe --members --group %s 2>&1',
                escapeshellarg(self::container()),
                escapeshellarg($group)
            ));
            if (str_contains($output, 'console-share-consumer') === $present) {
                return;
            }
            usleep(500000);
        } while (microtime(true) < $deadline);

        self::fail("The share group {$group} did not " . ($present ? 'get' : 'lose') . ' its member in time');
    }

    /**
     * Sends one frame on a fresh connection to the one node - the group coordinator of every group - and decodes it
     *
     * @template T of object
     *
     * @param class-string<T> $responseClass
     *
     * @return T
     */
    private function exchange(AbstractRequest $request, string $responseClass): object
    {
        $stream = $this->connect();
        $request->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    private function topic(string $purpose, int $partitions): string
    {
        $topic                 = self::uniqueTopicName("t4-41-share-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, $partitions, 1)]));

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

    private function group(string $purpose): string
    {
        $group                 = 't4-41-share-' . $purpose . '-group-' . bin2hex(random_bytes(6));
        $this->createdGroups[] = $group;

        return $group;
    }

    /**
     * Name of the container the node of this line runs in
     */
    private static function container(): string
    {
        $container = getenv('KAFKA_CONTAINER');

        return $container === false || trim($container) === '' ? 'kafka-4-3-1' : trim($container);
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ] + ConsumerConfig::getDefaultConfiguration();
    }
}
