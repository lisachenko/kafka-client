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
use Protocol\Kafka\Admin\LogDirInfo;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\ReplicaInfo;
use Protocol\Kafka\Admin\TopicPartitionReplica;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\BrokerNotAvailableException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\LogDirNotFoundException;
use Protocol\Kafka\Common\Errors\ReplicaNotAvailableException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsRequestLogDir;
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsRequestTopic;
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsResponsePartition;
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsResponseTopic;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsRequestTopic;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDir;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseTopic;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsRequest;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsResponse;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequest;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponse;

/**
 * Exercises the two JBOD apis of KIP-113 against a real Kafka 1.1.1 broker with **two** log directories.
 *
 * The container of `docker-compose.yml` runs with `log.dirs=/tmp/kafka-logs,/tmp/kafka-logs-2`, which is what makes
 * this suite possible at all: a broker with one directory can only ever answer "the replica is already there".
 * Every test moves replicas of a topic it created itself, because the broker is shared with the other suites.
 *
 * @see docs/protocol/1.1.md, sections "DescribeLogDirs API (key 35, v0)" and
 *      "AlterReplicaLogDirs API (key 34, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeLogDirsRequest::class)]
#[CoversClass(DescribeLogDirsResponse::class)]
#[CoversClass(DescribeLogDirsRequestTopic::class)]
#[CoversClass(DescribeLogDirsResponseLogDir::class)]
#[CoversClass(DescribeLogDirsResponseTopic::class)]
#[CoversClass(DescribeLogDirsResponsePartition::class)]
#[CoversClass(AlterReplicaLogDirsRequest::class)]
#[CoversClass(AlterReplicaLogDirsResponse::class)]
#[CoversClass(AlterReplicaLogDirsRequestLogDir::class)]
#[CoversClass(AlterReplicaLogDirsRequestTopic::class)]
#[CoversClass(AlterReplicaLogDirsResponseTopic::class)]
#[CoversClass(AlterReplicaLogDirsResponsePartition::class)]
#[CoversClass(LogDirInfo::class)]
#[CoversClass(ReplicaInfo::class)]
#[CoversClass(TopicPartitionReplica::class)]
final class LogDirsApiTest extends IntegrationTestCase
{
    /**
     * The two directories of `log.dirs` of `docker/kafka-1.1.1/start.sh`
     */
    private const string FIRST_DIR = '/tmp/kafka-logs';

    private const string SECOND_DIR = '/tmp/kafka-logs-2';

    /**
     * A path that is not in `log.dirs`, which is what the broker answers 57 for
     */
    private const string ABSENT_DIR = '/tmp/kafka-logs-there-is-no-such-directory';

    /**
     * How long to wait for a fresh topic to become servable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * How long to wait for a replica move to finish, in seconds
     */
    private const float MOVE_TIMEOUT = 60.0;

    /**
     * Records of one produce call: a whole batch has to stay below `message.max.bytes` (1 MB by default)
     */
    private const int BATCH_SIZE = 100;

    private const int RECORD_SIZE = 8192;

    /**
     * A log this big - 1000 records of 8 KB, about 8 MB - takes the mover long enough for a DescribeLogDirs sent
     * right after the answer of AlterReplicaLogDirs to see the future log. A 23 KB partition is moved in about
     * 0.1 s on the container, which is faster than one request round trip.
     */
    private const int LARGE_RECORD_COUNT = 1000;

    /**
     * Every other test only needs the partition to exist, so its log stays one batch
     */
    private const int SMALL_RECORD_COUNT = self::BATCH_SIZE;

    private Cluster $cluster;

    private AdminClient $admin;

    private Client $client;

    /**
     * Topics this test class created, deleted again after every test
     *
     * @var list<string>
     */
    private array $createdTopics = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
        $this->client  = new Client($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testTheBrokerReportsBothLogDirectoriesOfTheContainer(): void
    {
        $directories = $this->admin->describeLogDirs([$this->brokerId()], [])[$this->brokerId()];

        self::assertSame(
            [self::FIRST_DIR, self::SECOND_DIR],
            array_keys($directories),
            'the answer holds one entry per `log.dirs` entry, in the order the broker configured them'
        );
        foreach ($directories as $directory) {
            self::assertNull($directory->error, 'both directories of the container are online');
            self::assertSame([], $directory->replicaInfos, 'an empty topic array asks for no replica at all');
        }
    }

    public function testAPartitionIsReportedInExactlyOneDirectoryWithNoLagAndNoFutureFlag(): void
    {
        $topic = $this->topicWithRecords('describe');

        $directories = $this->admin->describeLogDirs([$this->brokerId()], [$topic => [0]])[$this->brokerId()];

        $holders = $this->directoriesHolding($directories, $topic);
        self::assertCount(1, $holders, 'a replica that is not being moved lives in exactly one directory');

        $replica = $directories[$holders[0]]->replica($topic, 0);
        self::assertInstanceOf(ReplicaInfo::class, $replica);
        self::assertGreaterThan(
            self::SMALL_RECORD_COUNT * self::RECORD_SIZE,
            $replica->size,
            'the size is the bytes of the log segments, which is at least the records themselves'
        );
        self::assertSame(0, $replica->offsetLag, 'the current log of a one-broker cluster is never behind');
        self::assertFalse($replica->isFuture);
    }

    public function testAnUnknownTopicIsNeitherAnErrorNorAnEntryAndIsNotCreated(): void
    {
        $absent = self::uniqueTopicName('t5-logdirs-absent');

        $directories = $this->admin->describeLogDirs([$this->brokerId()], [$absent => [0]])[$this->brokerId()];

        foreach ($directories as $directory) {
            self::assertNull($directory->error, 'an unknown replica is not a failure of the directory');
            self::assertSame([], $directory->replicaInfos, 'and it produces no entry at all - there is no code 3');
        }
        self::assertNotContains($absent, $this->admin->listTopics(), 'and the topic was not created');
    }

    public function testANullSelectionAsksForEveryReplicaOfTheBroker(): void
    {
        $topic = $this->topicWithRecords('all');

        $directories = $this->admin->describeLogDirs([$this->brokerId()])[$this->brokerId()];

        $everything = [];
        foreach ($directories as $directory) {
            $everything += $directory->replicaInfos;
        }

        self::assertArrayHasKey(
            LogDirInfo::keyOf($topic, 0),
            $everything,
            'the null topic array reports every replica of the broker, this one included'
        );
        self::assertArrayHasKey(
            LogDirInfo::keyOf('__consumer_offsets', 0),
            $everything,
            'and the internal topics of the cluster as well'
        );
        $named    = $this->admin->describeLogDirs([$this->brokerId()], [$topic => [0]])[$this->brokerId()];
        $selected = [];
        foreach ($named as $directory) {
            $selected += $directory->replicaInfos;
        }

        self::assertSame(1, count($selected), 'a named selection answers that one replica');
        self::assertGreaterThan(
            count($selected),
            count($everything),
            'which is why a client that knows its partitions should name them: the null array is the whole broker'
        );
    }

    public function testMovingAReplicaCreatesAFutureLogAndThenSwapsItIn(): void
    {
        $topic = $this->topicWithRecords('move', self::LARGE_RECORD_COUNT);

        [$source, $target, $directories] = $this->moveUntilTheFutureLogIsVisible($topic);

        $future  = $directories[$target]->replica($topic, 0);
        $current = $directories[$source]->replica($topic, 0);

        self::assertInstanceOf(ReplicaInfo::class, $future, 'the destination holds the future log while it fills');
        self::assertTrue($future->isFuture);
        self::assertGreaterThan(0, $future->offsetLag, 'which is still behind the log it copies');
        self::assertInstanceOf(ReplicaInfo::class, $current, 'and the source still serves the partition');
        self::assertFalse($current->isFuture);
        self::assertSame(0, $current->offsetLag, 'the log that is still served is never behind itself');

        $settled = $this->awaitMove($topic, 0, $target);

        self::assertSame([$target], $this->directoriesHolding($settled, $topic), 'the source lost its entry');
        self::assertFalse($settled[$target]->replica($topic, 0)?->isFuture, 'and the future log became the current one');
        self::assertSame(0, $settled[$target]->replica($topic, 0)?->offsetLag);
    }

    public function testAMoveIntoTheDirectoryTheReplicaAlreadySitsInIsAcceptedAndChangesNothing(): void
    {
        $topic     = $this->topicWithRecords('same');
        $directory = $this->directoryOf($topic, 0);

        $result = $this->admin->alterReplicaLogDirs([
            TopicPartitionReplica::of($topic, 0, $this->brokerId())->key() => $directory,
        ]);

        self::assertNull(
            $result[TopicPartitionReplica::of($topic, 0, $this->brokerId())->key()],
            'the api is idempotent: no future log is created when the destination is the current directory'
        );

        $directories = $this->admin->describeLogDirs([$this->brokerId()], [$topic => [0]])[$this->brokerId()];
        self::assertSame([$directory], $this->directoriesHolding($directories, $topic));
        self::assertFalse($directories[$directory]->replica($topic, 0)?->isFuture);
    }

    public function testARunningMoveIsCancelledByPointingItBackAtTheCurrentDirectory(): void
    {
        $topic = $this->topicWithRecords('cancel', self::LARGE_RECORD_COUNT);
        $key   = TopicPartitionReplica::of($topic, 0, $this->brokerId())->key();

        [$source, , ] = $this->moveUntilTheFutureLogIsVisible($topic);

        // `alterReplicaLogDirs` takes the same branch for every destination that differs from the one the future
        // log is in: it removes the fetcher, drops the future replica and deletes its log asynchronously
        self::assertNull($this->admin->alterReplicaLogDirs([$key => $source])[$key]);

        $settled = $this->awaitMove($topic, 0, $source);

        self::assertSame(
            [$source],
            $this->directoriesHolding($settled, $topic),
            'the future log was deleted and the partition stayed where it was'
        );
        self::assertFalse($settled[$source]->replica($topic, 0)?->isFuture);
    }

    public function testADirectoryThatIsNotInLogDirsIsLogDirNotFound(): void
    {
        $topic = $this->topicWithRecords('unknown-dir');
        $key   = TopicPartitionReplica::of($topic, 0, $this->brokerId())->key();

        $result = $this->admin->alterReplicaLogDirs([$key => self::ABSENT_DIR]);

        self::assertInstanceOf(LogDirNotFoundException::class, $result[$key]);
        self::assertSame(KafkaException::LOG_DIR_NOT_FOUND, $result[$key]->getCode());
    }

    public function testARelativePathIsAlsoLogDirNotFound(): void
    {
        // `LogManager.isLogDirOnline` compares the value against `logDirs.map(_.getAbsolutePath)`, so a relative
        // path is refused even when it names one of the two directories the container really has
        $topic = $this->topicWithRecords('relative');
        $key   = TopicPartitionReplica::of($topic, 0, $this->brokerId())->key();

        $result = $this->admin->alterReplicaLogDirs([$key => ltrim(self::FIRST_DIR, '/')]);

        self::assertInstanceOf(LogDirNotFoundException::class, $result[$key]);
    }

    public function testAReplicaTheBrokerDoesNotHostIsReplicaNotAvailableAndNoTopicIsCreated(): void
    {
        $absent = self::uniqueTopicName('t5-logdirs-no-replica');
        $key    = TopicPartitionReplica::of($absent, 0, $this->brokerId())->key();

        $result = $this->admin->alterReplicaLogDirs([$key => self::FIRST_DIR]);

        self::assertInstanceOf(
            ReplicaNotAvailableException::class,
            $result[$key],
            'the topic apis answer 3 for an unknown topic, this one answers 9'
        );
        self::assertNotContains($absent, $this->admin->listTopics());
    }

    public function testAPartitionTheTopicDoesNotHaveIsAlsoReplicaNotAvailable(): void
    {
        $topic = $this->topicWithRecords('no-partition');
        $key   = TopicPartitionReplica::of($topic, 7, $this->brokerId())->key();

        $result = $this->admin->alterReplicaLogDirs([$key => self::SECOND_DIR]);

        self::assertInstanceOf(ReplicaNotAvailableException::class, $result[$key]);
    }

    public function testABrokerIdTheClusterDoesNotHaveIsRefusedBeforeAnythingIsSent(): void
    {
        // Both apis are broker-local, so there is no broker that could answer for a node id the cluster has never
        // had - asking a different one would describe or move ITS replicas
        $this->expectException(BrokerNotAvailableException::class);

        $this->admin->describeLogDirs([$this->brokerId() + 4242]);
    }

    public function testTheRawFramesOfBothApisMatchTheDocumentedGrammar(): void
    {
        $topic  = $this->topicWithRecords('raw');
        $stream = $this->connect();

        new DescribeLogDirsRequest([$topic => [0]], 't5-logdirs', 5150)->writeTo($stream);
        $described = DescribeLogDirsResponse::unpack($stream);

        self::assertSame(5150, $described->getCorrelationId());
        self::assertSame(0, $described->throttleTimeMs, 'the container sets no quota');
        self::assertSame([self::FIRST_DIR, self::SECOND_DIR], array_keys($described->logDirs));

        new AlterReplicaLogDirsRequest(
            [self::ABSENT_DIR => [$topic => [0]]],
            't5-logdirs',
            5151
        )->writeTo($stream);
        $altered = AlterReplicaLogDirsResponse::unpack($stream);

        self::assertSame(5151, $altered->getCorrelationId());
        self::assertSame(
            KafkaException::LOG_DIR_NOT_FOUND,
            $altered->topics[$topic]->partitions[0]->errorCode,
            'the error is per replica, the api has no top-level error code'
        );
    }

    /**
     * Starts a replica move and returns the answer in which its future log is visible
     *
     * The copy of an 8 MB log takes about a quarter of a second on the container, so a DescribeLogDirs sent right
     * after the answer of AlterReplicaLogDirs normally sees the future log. The **first** move of a freshly
     * written topic is faster than that, though - measured at about 100 ms, less than the round trip of the
     * describe that follows it - so the move is repeated, back and forth between the two directories, until one of
     * them is caught in flight.
     *
     * @return array{0: string, 1: string, 2: array<string, LogDirInfo>} The source, the destination, and the
     *         DescribeLogDirs answer that reports the partition in both of them
     */
    private function moveUntilTheFutureLogIsVisible(string $topic): array
    {
        $key      = TopicPartitionReplica::of($topic, 0, $this->brokerId())->key();
        $deadline = microtime(true) + self::MOVE_TIMEOUT;

        do {
            $source = $this->directoryOf($topic, 0);
            $target = $source === self::FIRST_DIR ? self::SECOND_DIR : self::FIRST_DIR;

            self::assertSame(
                [$key => null],
                $this->admin->alterReplicaLogDirs([$key => $target]),
                'the answer only says that the move was accepted'
            );

            // The future log is created inside the request handler, so the answer means it already exists
            $directories = $this->admin->describeLogDirs([$this->brokerId()], [$topic => [0]])[$this->brokerId()];
            if ($directories[$target]->replica($topic, 0)?->isFuture === true) {
                return [$source, $target, $directories];
            }

            $this->awaitMove($topic, 0, $target);
        } while (microtime(true) < $deadline);

        self::fail("No move of {$topic}-0 was caught while its future log was still filling");
    }

    /**
     * Waits until the partition is reported in the target directory alone, i.e. the mover swapped the logs in
     *
     * @return array<string, LogDirInfo>
     */
    private function awaitMove(string $topic, int $partition, string $target): array
    {
        $deadline = microtime(true) + self::MOVE_TIMEOUT;
        do {
            $directories = $this->admin->describeLogDirs([$this->brokerId()], [$topic => [$partition]])
                [$this->brokerId()];
            $holders     = $this->directoriesHolding($directories, $topic);
            if ($holders === [$target] && $directories[$target]->replica($topic, $partition)?->isFuture === false) {
                return $directories;
            }
            usleep(50000);
        } while (microtime(true) < $deadline);

        self::fail("The replica {$topic}-{$partition} did not arrive in {$target} within " . self::MOVE_TIMEOUT . 's');
    }

    /**
     * Returns the directories that report a replica of the given topic, in the order of the answer
     *
     * @param array<string, LogDirInfo> $directories
     *
     * @return list<string>
     */
    private function directoriesHolding(array $directories, string $topic): array
    {
        $holders = [];
        foreach ($directories as $path => $directory) {
            foreach (array_keys($directory->replicaInfos) as $key) {
                if (str_starts_with($key, $topic . '-')) {
                    $holders[] = $path;

                    break;
                }
            }
        }

        return $holders;
    }

    /**
     * Returns the directory one partition currently lives in
     */
    private function directoryOf(string $topic, int $partition): string
    {
        $directories = $this->admin->describeLogDirs(
            [$this->brokerId()],
            [new TopicPartition($topic, $partition)]
        )[$this->brokerId()];

        foreach ($directories as $path => $directory) {
            if ($directory->replica($topic, $partition) !== null) {
                return $path;
            }
        }

        self::fail("No log directory of the broker holds {$topic}-{$partition}");
    }

    /**
     * Node id of the single broker of the container
     */
    private function brokerId(): int
    {
        return array_key_first($this->cluster->nodes());
    }

    /**
     * Creates a topic of one partition and fills it with records, returning its name
     *
     * The records are written in batches of {@see self::BATCH_SIZE}, because one produce request has to stay below
     * the `message.max.bytes` of the broker - 1 MB by default, and 100 records of 8 KB are 800 KB.
     */
    private function topicWithRecords(string $purpose, int $records = self::SMALL_RECORD_COUNT): string
    {
        $topic                 = self::uniqueTopicName("t5-logdirs-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

        // Every record is stamped with `now`: a time-based retention deletes a segment by the largest timestamp it
        // holds, so a record from the past could take the whole segment with it while the test runs
        $now     = (int) (microtime(true) * 1000);
        $payload = str_repeat('t5', intdiv(self::RECORD_SIZE, 2));

        for ($written = 0; $written < $records; $written += self::BATCH_SIZE) {
            $batch = array_fill(
                0,
                min(self::BATCH_SIZE, $records - $written),
                new Record($payload, null, 0, null, $now)
            );
            $this->produceWithRetries($topic, $batch);
        }

        return $topic;
    }

    /**
     * Produces one batch, retrying while the fresh topic is still answering 5 or 6
     *
     * @param list<Record> $messages
     */
    private function produceWithRetries(string $topic, array $messages): void
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->client->produce([$topic => [0 => $messages]]);

                return;
            } catch (TopicPartitionRequestException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
                $this->cluster->reload();
            }
        } while (true);
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
            ClientConfig::CLIENT_ID                 => 't5-logdirs',
            ClientConfig::REQUEST_TIMEOUT_MS        => 60000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => 1,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
