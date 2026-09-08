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

namespace Protocol\Kafka\Tests\Unit\Consumer;

use BadMethodCallException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\RecordTooLargeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\Internals\SubscriptionState;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Tests\Unit\Consumer\Fixture\FakeClient;
use Protocol\Kafka\Tests\Unit\Consumer\Fixture\JsonDeserializer;
use Protocol\Kafka\Tests\Unit\Consumer\Fixture\TestKafkaConsumer;

/**
 * Drives the consumer against an in-memory client, so that every decision it makes on its own - the offset reset,
 * the auto-commit timing, the position bookkeeping, the deserialization and the pausing - is observable without a
 * broker.
 */
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(ConsumerRecord::class)]
#[CoversClass(ConsumerConfig::class)]
#[CoversClass(RecordTooLargeException::class)]
final class KafkaConsumerTest extends TestCase
{
    private const string TOPIC = 't9-unit-topic';

    private const string GROUP = 't9-unit-group';

    public function testCheckCrcsCanNotBeSwitchedOff(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessageMatches('/check\.crcs/');

        new TestKafkaConsumer(new FakeClient(), $this->configuration([ConsumerConfig::CHECK_CRCS => false]));
    }

    public function testAutomaticCommitRequiresAGroup(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessageMatches('/group\.id/');

        new TestKafkaConsumer(new FakeClient(), [
            ConsumerConfig::GROUP_ID           => '',
            ConsumerConfig::ENABLE_AUTO_COMMIT => true,
        ]);
    }

    public function testAConfiguredDeserializerHasToImplementTheInterface(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        new TestKafkaConsumer(new FakeClient(), $this->configuration([
            ConsumerConfig::VALUE_DESERIALIZER => \stdClass::class,
        ]));
    }

    public function testSubscribeIsNotSupportedByTheProtocolLine(): void
    {
        $consumer = new TestKafkaConsumer(new FakeClient(), $this->configuration());

        self::assertSame([], $consumer->subscription());

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessageMatches('/assign\(\)/');
        $consumer->subscribe([self::TOPIC]);
    }

    public function testAssignmentIsReportedBack(): void
    {
        $client = $this->clientWithLog([0 => 3, 1 => 2]);

        $consumer = $this->consumer($client);
        $consumer->assign([self::TOPIC => [0, 1]]);

        self::assertSame([self::TOPIC => [0 => 0, 1 => 1]], $consumer->assignment());
    }

    public function testAssignmentAcceptsTheProtocolDto(): void
    {
        $client   = $this->clientWithLog([0 => 1]);
        $consumer = $this->consumer($client);

        $consumer->assign([self::TOPIC => new PartitionsForTopic(self::TOPIC, [0])]);

        self::assertSame([self::TOPIC => [0 => 0]], $consumer->assignment());
    }

    public function testAssignmentOfAnEmptyListUnsubscribes(): void
    {
        $client   = $this->clientWithLog([0 => 1]);
        $consumer = $this->consumer($client);
        $consumer->assign([self::TOPIC => [0]]);

        $consumer->assign([]);

        self::assertSame([], $consumer->assignment());
    }

    public function testPositionOfANewGroupFollowsTheEarliestResetStrategy(): void
    {
        $client = $this->clientWithLog([0 => 5]);
        $client->logStartOffsets[self::TOPIC][0] = 0;

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET => OffsetResetStrategy::EARLIEST,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        self::assertSame(0, $consumer->position(self::TOPIC, 0));
    }

    public function testPositionOfANewGroupFollowsTheLatestResetStrategy(): void
    {
        $client = $this->clientWithLog([0 => 5]);

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET => OffsetResetStrategy::LATEST,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        self::assertSame(5, $consumer->position(self::TOPIC, 0));
        self::assertSame(
            [self::TOPIC => [0 => []]],
            $consumer->poll(10),
            'a consumer at the end of the log receives no record'
        );
    }

    public function testMissingCommittedOffsetIsAnErrorWhenNoResetStrategyIsConfigured(): void
    {
        $client   = $this->clientWithLog([0 => 5]);
        $consumer = $this->consumer($client, [ConsumerConfig::AUTO_OFFSET_RESET => OffsetResetStrategy::NONE]);

        $this->expectException(OffsetOutOfRangeException::class);
        $consumer->assign([self::TOPIC => [0]]);
    }

    public function testCommittedOffsetOfTheGroupBecomesThePosition(): void
    {
        $client = $this->clientWithLog([0 => 5]);
        $client->committedOffsets[self::GROUP][self::TOPIC][0] = 3;

        $consumer = $this->consumer($client);
        $consumer->assign([self::TOPIC => [0]]);

        self::assertSame(3, $consumer->position(self::TOPIC, 0));

        $records = $consumer->poll(10)[self::TOPIC][0];

        self::assertCount(2, $records);
        self::assertSame([3, 4], array_map(static fn(Record $record): int => (int) $record->offset, $records));
    }

    public function testPollAdvancesThePositionBehindTheLastRecord(): void
    {
        $client   = $this->clientWithLog([0 => 3]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $first = $consumer->poll(10);

        self::assertCount(3, $first[self::TOPIC][0]);
        self::assertSame(3, $consumer->position(self::TOPIC, 0));

        $client->append(self::TOPIC, 0, ['value-3']);
        $second = $consumer->poll(10);

        self::assertCount(1, $second[self::TOPIC][0]);
        self::assertSame('value-3', $second[self::TOPIC][0][0]->value);
        self::assertSame(4, $consumer->position(self::TOPIC, 0));
    }

    public function testRecordsBeforeThePositionAreDiscarded(): void
    {
        $client = $this->clientWithLog([0 => 5]);
        // A compressed message set is handed back as a whole, records before the fetch offset included
        $client->ignoreFetchOffset = true;
        $client->committedOffsets[self::GROUP][self::TOPIC][0] = 3;

        $consumer = $this->consumer($client);
        $consumer->assign([self::TOPIC => [0]]);

        $records = $consumer->poll(10)[self::TOPIC][0];

        self::assertSame([3, 4], array_map(static fn(Record $record): int => (int) $record->offset, $records));
        self::assertSame(5, $consumer->position(self::TOPIC, 0));
    }

    public function testAutomaticCommitHappensOnEveryPollWithoutAnInterval(): void
    {
        $client   = $this->clientWithLog([0 => 2]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => true,
            ConsumerConfig::AUTO_COMMIT_INTERVAL_MS => 0,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $consumer->poll(10);
        $consumer->poll(10);

        self::assertCount(2, $client->commits);
        self::assertSame(self::GROUP, $client->commits[0]['group']);
        self::assertSame([self::TOPIC => [0 => 2]], $client->commits[0]['offsets']);
        self::assertSame(2, $client->committedOffsets[self::GROUP][self::TOPIC][0]);
    }

    public function testAutomaticCommitWaitsForTheConfiguredInterval(): void
    {
        $client   = $this->clientWithLog([0 => 2]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => true,
            ConsumerConfig::AUTO_COMMIT_INTERVAL_MS => 3600000,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $consumer->poll(10);
        $client->append(self::TOPIC, 0, ['value-2']);
        $consumer->poll(10);
        $consumer->poll(10);

        self::assertCount(1, $client->commits, 'the interval did not elapse between the polls');
        self::assertSame([self::TOPIC => [0 => 2]], $client->commits[0]['offsets']);
    }

    public function testAutomaticCommitCanBeSwitchedOff(): void
    {
        $client   = $this->clientWithLog([0 => 2]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $consumer->poll(10);
        $consumer->poll(10);

        self::assertSame([], $client->commits);
    }

    public function testCommitSyncSendsThePositionsOfTheConsumer(): void
    {
        $client   = $this->clientWithLog([0 => 2, 1 => 3]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0, 1]]);
        $consumer->poll(10);

        $consumer->commitSync();

        self::assertSame([self::TOPIC => [0 => 2, 1 => 3]], $client->commits[0]['offsets']);
        self::assertSame(
            [self::TOPIC => [0 => 2, 1 => 3]],
            $consumer->committed([self::TOPIC => [0, 1]])
        );
    }

    public function testCommitSyncAcceptsExplicitOffsetsWithMetadata(): void
    {
        $client   = $this->clientWithLog([0 => 5]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $consumer->commitSync([self::TOPIC => [0 => new OffsetAndMetadata(4, 'checkpoint')]]);

        self::assertSame(4, $client->committedOffsets[self::GROUP][self::TOPIC][0]);
        self::assertSame(0, $consumer->position(self::TOPIC, 0), 'an explicit commit does not move the position');
    }

    public function testCommitSyncWithoutAnythingToCommitDoesNotTalkToTheBroker(): void
    {
        $client   = new FakeClient();
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);

        $consumer->commitSync();

        self::assertSame([], $client->commits);
    }

    public function testCommittedOffsetOfAnUnknownPartitionIsMinusOne(): void
    {
        $client   = $this->clientWithLog([0 => 1]);
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);

        self::assertSame([self::TOPIC => [0 => -1]], $consumer->committed([self::TOPIC => [0]]));
    }

    public function testSeekOverridesThePositionOfTheNextPoll(): void
    {
        $client   = $this->clientWithLog([0 => 5]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::LATEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $consumer->seek(self::TOPIC, 0, 4);

        self::assertSame(4, $consumer->position(self::TOPIC, 0));
        self::assertCount(1, $consumer->poll(10)[self::TOPIC][0]);
    }

    public function testSeekOfANotAssignedPartitionIsRejected(): void
    {
        $client   = $this->clientWithLog([0 => 5]);
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->assign([self::TOPIC => [0]]);

        $this->expectException(UnknownTopicOrPartitionException::class);
        $consumer->seek(self::TOPIC, 5, 0);
    }

    public function testSeekToBeginningAndToEndUseTheBoundsOfTheLog(): void
    {
        $client = $this->clientWithLog([0 => 5]);
        $client->logStartOffsets[self::TOPIC][0] = 2;

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->assign([self::TOPIC => [0]]);

        $consumer->seekToBeginning([self::TOPIC => [0]]);
        self::assertSame(2, $consumer->position(self::TOPIC, 0));

        $consumer->seekToEnd([self::TOPIC => [0]]);
        self::assertSame(5, $consumer->position(self::TOPIC, 0));
    }

    public function testSeekToBeginningOfANotAssignedTopicIsRejected(): void
    {
        $client   = $this->clientWithLog([0 => 5]);
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->assign([self::TOPIC => [0]]);

        $this->expectException(UnknownTopicOrPartitionException::class);
        $consumer->seekToBeginning(['another-topic' => [0]]);
    }

    public function testPausedPartitionIsNotFetchedUntilItIsResumed(): void
    {
        $client   = $this->clientWithLog([0 => 2, 1 => 2]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0, 1]]);

        $consumer->pause([self::TOPIC => [1]]);
        $paused = $consumer->poll(10);

        self::assertArrayHasKey(0, $paused[self::TOPIC]);
        self::assertArrayNotHasKey(1, $paused[self::TOPIC]);
        self::assertSame(0, $consumer->position(self::TOPIC, 1), 'a paused partition keeps its position');

        $consumer->resume([self::TOPIC => [1]]);
        $resumed = $consumer->poll(10);

        self::assertCount(2, $resumed[self::TOPIC][1]);
        self::assertSame(2, $consumer->position(self::TOPIC, 1));
    }

    public function testOffsetOutOfRangeIsResetToTheBeginningOfTheLog(): void
    {
        $client = $this->clientWithLog([0 => 3], 100);
        $client->committedOffsets[self::GROUP][self::TOPIC][0] = 5;
        $client->fetchFailures = [new OffsetOutOfRangeException(['topic' => self::TOPIC])];

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        self::assertSame(5, $consumer->position(self::TOPIC, 0), 'the stale committed offset is used first');

        $records = $consumer->poll(10)[self::TOPIC][0];

        self::assertCount(3, $records);
        self::assertSame(100, (int) $records[0]->offset);
        self::assertSame(103, $consumer->position(self::TOPIC, 0));
    }

    public function testOffsetOutOfRangeIsResetToTheEndOfTheLog(): void
    {
        $client = $this->clientWithLog([0 => 3], 100);
        $client->committedOffsets[self::GROUP][self::TOPIC][0] = 5;
        $client->fetchFailures = [new OffsetOutOfRangeException(['topic' => self::TOPIC])];

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::LATEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);
        $result = $consumer->poll(10);

        self::assertSame([], $result[self::TOPIC][0]);
        self::assertSame(103, $consumer->position(self::TOPIC, 0));
    }

    public function testOffsetOutOfRangeIsRethrownWhenNoResetStrategyIsConfigured(): void
    {
        $client = $this->clientWithLog([0 => 3], 100);
        $client->committedOffsets[self::GROUP][self::TOPIC][0] = 5;
        $client->fetchFailures = [new OffsetOutOfRangeException(['topic' => self::TOPIC])];

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);
        $consumer->seek(self::TOPIC, 0, 5);

        // The reset strategy is only consulted after the assignment, so it is changed on a fresh consumer here
        $strict = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::NONE,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $strict->assign([self::TOPIC => [0]]);
        $client->fetchFailures = [new OffsetOutOfRangeException(['topic' => self::TOPIC])];

        $this->expectException(OffsetOutOfRangeException::class);
        $strict->poll(10);
    }

    public function testPartitionedFailureWithoutAnOutOfRangeErrorIsPropagated(): void
    {
        $client = $this->clientWithLog([0 => 3]);
        $client->fetchFailures = [
            new TopicPartitionRequestException([], [self::TOPIC => [0 => new UnknownTopicOrPartitionException([])]]),
        ];

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $this->expectException(TopicPartitionRequestException::class);
        $consumer->poll(10);
    }

    public function testPartitionedOutOfRangeFailureIsResetAndRetried(): void
    {
        $client = $this->clientWithLog([0 => 3], 100);
        $client->committedOffsets[self::GROUP][self::TOPIC][0] = 5;
        $client->fetchFailures = [
            new TopicPartitionRequestException([], [self::TOPIC => [0 => new OffsetOutOfRangeException([])]]),
        ];

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        self::assertCount(3, $consumer->poll(10)[self::TOPIC][0]);
    }

    public function testAPartitionStuckOnATooLargeMessageIsReported(): void
    {
        $client = new FakeClient();
        $client->logStartOffsets[self::TOPIC][0] = 0;
        $client->logEndOffsets[self::TOPIC][0]   = 4;

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET         => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT        => false,
            ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 64,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        // The first empty answer is indistinguishable from an idle partition, so it costs nothing
        self::assertSame([self::TOPIC => [0 => []]], $consumer->poll(10));

        $this->expectException(RecordTooLargeException::class);
        $this->expectExceptionMessageMatches('/max\.partition\.fetch\.bytes/');
        $consumer->poll(10);
    }

    public function testAnIdlePartitionIsNotReportedAsStuck(): void
    {
        $client   = $this->clientWithLog([0 => 2]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $consumer->poll(10);
        $consumer->poll(10);
        $consumer->poll(10);

        self::assertSame(2, $consumer->position(self::TOPIC, 0));
    }

    public function testDeserializersAreAppliedToTheKeyAndTheValue(): void
    {
        $client = new FakeClient();
        $client->log[self::TOPIC][0] = [new Record('{"id":7}', 'key-7', 0, 0)];

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
            ConsumerConfig::KEY_DESERIALIZER   => new StringDeserializer(),
            ConsumerConfig::VALUE_DESERIALIZER => JsonDeserializer::class,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $record = $consumer->poll(10)[self::TOPIC][0][0];

        self::assertInstanceOf(ConsumerRecord::class, $record);
        self::assertSame(self::TOPIC, $record->topic);
        self::assertSame(0, $record->partition);
        self::assertSame('{"id":7}', $record->value, 'the raw bytes stay available');
        self::assertSame('key-7', $record->deserializedKey);
        self::assertSame(['id' => 7], $record->deserializedValue);
    }

    public function testRecordsAreNotWrappedWithoutADeserializer(): void
    {
        $client   = $this->clientWithLog([0 => 1]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $record = $consumer->poll(10)[self::TOPIC][0][0];

        self::assertInstanceOf(Record::class, $record);
        self::assertNotInstanceOf(ConsumerRecord::class, $record);
    }

    public function testANullValueIsPassedThroughTheDeserializer(): void
    {
        $client = new FakeClient();
        $client->log[self::TOPIC][0] = [new Record(null, null, 0, 0)];

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
            ConsumerConfig::VALUE_DESERIALIZER => JsonDeserializer::class,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        $record = $consumer->poll(10)[self::TOPIC][0][0];

        self::assertInstanceOf(ConsumerRecord::class, $record);
        self::assertNull($record->deserializedValue);
    }

    public function testUnsubscribeDropsTheAssignmentWithoutTouchingTheCommittedOffsets(): void
    {
        $client   = $this->clientWithLog([0 => 2]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);
        $consumer->poll(10);
        $consumer->commitSync();

        $consumer->unsubscribe();

        self::assertSame([], $consumer->assignment());
        self::assertSame([], $consumer->poll(10));
        self::assertSame(2, $client->committedOffsets[self::GROUP][self::TOPIC][0]);
    }

    public function testAConsumerWithoutAGroupSeeksWithoutAskingForCommittedOffsets(): void
    {
        $client   = $this->clientWithLog([0 => 4]);
        $consumer = new TestKafkaConsumer($client, [
            ConsumerConfig::GROUP_ID           => '',
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
        ]);

        $consumer->assign([self::TOPIC => [0]]);

        self::assertSame(0, $consumer->position(self::TOPIC, 0));
        self::assertCount(4, $consumer->poll(10)[self::TOPIC][0]);

        $this->expectException(InvalidConfigurationException::class);
        $consumer->commitSync([self::TOPIC => [0 => 1]]);
    }

    public function testTheSubscriptionStateStaysUserAssigned(): void
    {
        $client   = $this->clientWithLog([0 => 1]);
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->assign([self::TOPIC => [0]]);

        self::assertSame([], $consumer->subscription());
        self::assertNotSame(SubscriptionState::TYPE_NONE, SubscriptionState::TYPE_USER_ASSIGNED);
    }

    /**
     * Builds a fake client whose log holds the given number of records per partition
     *
     * @param array<int, int> $partitionRecordCounts Partition id => number of records in it
     * @param int             $firstOffset           Offset of the first record of every partition
     */
    private function clientWithLog(array $partitionRecordCounts, int $firstOffset = 0): FakeClient
    {
        $client = new FakeClient();
        foreach ($partitionRecordCounts as $partition => $count) {
            $client->logStartOffsets[self::TOPIC][$partition] = $firstOffset;
            $client->append(
                self::TOPIC,
                $partition,
                array_map(static fn(int $index): string => 'value-' . $index, range(0, $count - 1))
            );
        }

        return $client;
    }

    /**
     * @param array<string, mixed> $configuration Options that override the defaults of this test class
     */
    private function consumer(FakeClient $client, array $configuration = []): TestKafkaConsumer
    {
        return new TestKafkaConsumer($client, $this->configuration($configuration));
    }

    /**
     * @param array<string, mixed> $overrides Options that override the defaults of this test class
     *
     * @return array<string, mixed>
     */
    private function configuration(array $overrides = []): array
    {
        return $overrides + [
            ConsumerConfig::GROUP_ID                => self::GROUP,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => true,
            ConsumerConfig::AUTO_COMMIT_INTERVAL_MS => 0,
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
        ];
    }
}
