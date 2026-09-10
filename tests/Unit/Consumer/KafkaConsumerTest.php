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

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\RebalanceInProgressException;
use Protocol\Kafka\Common\Errors\RecordTooLargeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\Internals\ConsumerCoordinator;
use Protocol\Kafka\Consumer\Internals\SubscriptionState;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Consumer\OffsetAndTimestamp;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Consumer\RoundRobinAssignor;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Tests\Unit\Consumer\Fixture\FakeClient;
use Protocol\Kafka\Tests\Unit\Consumer\Fixture\JsonDeserializer;
use Protocol\Kafka\Tests\Unit\Consumer\Fixture\RecordingRebalanceListener;
use Protocol\Kafka\Tests\Unit\Consumer\Fixture\TestKafkaConsumer;

/**
 * Drives the consumer against an in-memory client, so that every decision it makes on its own - the offset reset,
 * the auto-commit timing, the position bookkeeping, the deserialization and the pausing - is observable without a
 * broker.
 */
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(ConsumerCoordinator::class)]
#[CoversClass(SubscriptionState::class)]
#[CoversClass(ConsumerRecord::class)]
#[CoversClass(OffsetAndTimestamp::class)]
#[CoversClass(ConsumerConfig::class)]
#[CoversClass(RecordTooLargeException::class)]
final class KafkaConsumerTest extends TestCase
{
    private const string TOPIC = 't9-unit-topic';

    private const string GROUP = 't9-unit-group';

    public function testCheckCrcsIsOnByDefaultAndMayBeSwitchedOff(): void
    {
        self::assertTrue(ConsumerConfig::getDefaultConfiguration()[ConsumerConfig::CHECK_CRCS]);

        // The client threads the option into MessageSet::fromBuffer(), so a consumer that trusts its network can
        // skip the checksum of every message it reads
        $client   = $this->clientWithLog([0 => 2]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::CHECK_CRCS         => false,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        self::assertCount(2, $consumer->poll(10)[self::TOPIC][0]);
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

        // This consumer is not a member of a broker-side group yet, so it commits as a "simple consumer" and asks
        // for the retention that the broker is configured with
        self::assertSame(OffsetCommitRequest::DEFAULT_MEMBER_NAME, $client->commits[0]['memberId']);
        self::assertSame(OffsetCommitRequest::DEFAULT_GENERATION_ID, $client->commits[0]['generationId']);
        self::assertSame(OffsetCommitRequest::DEFAULT_RETENTION_TIME, $client->commits[0]['retentionTime']);
    }

    public function testOffsetRetentionOptionIsPassedOnToTheCommit(): void
    {
        $client   = $this->clientWithLog([0 => 2]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
            ConsumerConfig::OFFSET_RETENTION_MS => 3600000,
        ]);
        $consumer->assign([self::TOPIC => [0]]);
        $consumer->poll(10);

        $consumer->commitSync();

        self::assertSame(3600000, $client->commits[0]['retentionTime']);
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

    public function testBeginningOffsetsAndEndOffsetsReportTheBoundsOfPartitionsThatAreNotAssigned(): void
    {
        $client = $this->clientWithLog([0 => 5, 1 => 2]);
        $client->logStartOffsets[self::TOPIC][0] = 2;

        // Neither method needs an assignment, exactly as in the Java consumer of Kafka 0.10.1
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);

        self::assertSame([self::TOPIC => [0 => 2, 1 => 0]], $consumer->beginningOffsets([self::TOPIC => [0, 1]]));
        self::assertSame([self::TOPIC => [0 => 5, 1 => 2]], $consumer->endOffsets([self::TOPIC => [0, 1]]));
        self::assertSame(
            [
                [self::TOPIC => [0 => OffsetsRequest::EARLIEST, 1 => OffsetsRequest::EARLIEST]],
                [self::TOPIC => [0 => OffsetsRequest::LATEST, 1 => OffsetsRequest::LATEST]],
            ],
            $client->offsetsCalls,
            'the two special target times of the Offsets api are what the broker is asked for'
        );
    }

    public function testBeginningOffsetsAndEndOffsetsOfNothingAskTheBrokerNothing(): void
    {
        $client   = $this->clientWithLog([0 => 1]);
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);

        self::assertSame([], $consumer->beginningOffsets([]));
        self::assertSame([], $consumer->endOffsets([]));
        self::assertSame([], $consumer->offsetsForTimes([]));
        self::assertSame([], $client->offsetsCalls);
    }

    public function testOffsetsForTimesReturnsTheFirstRecordAtOrAfterTheTimestamp(): void
    {
        $client   = $this->clientWithTimestampedLog();
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);

        $found = $consumer->offsetsForTimes([self::TOPIC => [0 => 1600000001500]]);

        $offsetAndTimestamp = $found[self::TOPIC][0];
        self::assertInstanceOf(OffsetAndTimestamp::class, $offsetAndTimestamp);
        self::assertSame(2, $offsetAndTimestamp->offset);
        self::assertSame(
            1600000002000,
            $offsetAndTimestamp->timestamp,
            'the timestamp of the message that was found, not the one that was searched for'
        );
    }

    public function testOffsetsForTimesReportsNullForAPartitionWithoutAMatchingRecord(): void
    {
        $client   = $this->clientWithTimestampedLog();
        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);

        // Above the timestamp of every message of the log, which the broker answers with the offset -1 and no error
        $found = $consumer->offsetsForTimes([self::TOPIC => [0 => 1600000009000]]);

        self::assertNull($found[self::TOPIC][0]);
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
        $client->logStartOffsets[self::TOPIC][0]   = 0;
        $client->logEndOffsets[self::TOPIC][0]     = 4;
        $client->oversizedMessages[self::TOPIC][0] = true;

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET         => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT        => false,
            ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 64,
        ]);
        $consumer->assign([self::TOPIC => [0]]);

        // The broker says so in the very answer, so the first poll already refuses instead of spinning
        try {
            $consumer->poll(10);
            self::fail('A partition that can not make progress has to be reported');
        } catch (RecordTooLargeException $exception) {
            self::assertSame(self::TOPIC, $exception->topic);
            self::assertSame(0, $exception->partition);
            self::assertSame(0, $exception->fetchOffset);
            self::assertSame(64, $exception->maxBytes);
            self::assertSame(4, $exception->logEndOffset);
            self::assertStringContainsString('max.partition.fetch.bytes', $exception->getMessage());
        }
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

    public function testThePartitionsThatReturnedRecordsAreAskedForLastInTheNextPoll(): void
    {
        // A Fetch v3 request is bounded by `fetch.max.bytes` for the whole answer and the broker fills the
        // partitions in the order of the request, so a consumer that always asked in the same order would starve
        // the partitions at its end. Every partition that returned records therefore moves behind the ones that
        // did not, which is what the Java consumer does with `SubscriptionState.movePartitionToEnd`.
        $client = $this->clientWithLog([0 => 1, 1 => 1]);
        // The third partition exists but its log is empty, so it is the one that comes back without records
        $client->logStartOffsets[self::TOPIC][2] = 0;

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0, 1, 2]]);

        $consumer->poll(10);
        $consumer->poll(10);

        self::assertSame([0, 1, 2], array_keys($client->fetchCalls[0][self::TOPIC]));
        self::assertSame(
            [2, 0, 1],
            array_keys($client->fetchCalls[1][self::TOPIC]),
            'the partition that had nothing to give is asked first, the served ones keep their relative order'
        );
    }

    public function testAnAnswerThatLeavesPartitionsOutKeepsTheirPositionAndTheirPlaceInTheFetchOrder(): void
    {
        // The consumer fetches through an incremental fetch session (KIP-227), so a partition that has nothing new
        // is simply not in the answer - and everything the consumer knows about it is still valid
        $client                              = $this->clientWithLog([0 => 2, 1 => 1]);
        $client->answerOnlyChangedPartitions = true;

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0, 1]]);
        $consumer->poll(10);

        // Both logs are read to their end, so the second answer carries the partition 1 and nothing else
        $client->append(self::TOPIC, 1, ['value-1']);
        $second = $consumer->poll(10);

        self::assertSame([self::TOPIC => [1]], array_map(array_keys(...), $second));
        self::assertSame(2, $consumer->position(self::TOPIC, 0), 'the partition that was left out keeps its place');
        self::assertSame(2, $consumer->position(self::TOPIC, 1));

        $consumer->poll(10);

        self::assertSame(
            [0 => 2, 1 => 2],
            $client->fetchCalls[2][self::TOPIC],
            'the positions of every partition are stated again, the session leaves out what did not move'
        );
    }

    public function testAPartitionOfAFreshAssignmentIsAppendedBehindTheKnownOrder(): void
    {
        $client   = $this->clientWithLog([0 => 1, 1 => 1]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0, 1]]);
        $consumer->poll(10);

        $client->append(self::TOPIC, 2, ['value-0']);
        $consumer->assign([self::TOPIC => [0, 1, 2]]);
        $consumer->poll(10);

        self::assertSame(
            [0, 1, 2],
            array_keys($client->fetchCalls[1][self::TOPIC]),
            'a partition this consumer never fetched is asked for behind the ones it knows'
        );
    }

    public function testAPausedPartitionKeepsItsPlaceInTheFetchOrder(): void
    {
        $client   = $this->clientWithLog([0 => 1, 1 => 1]);
        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([self::TOPIC => [0, 1]]);
        $consumer->pause([self::TOPIC => [1]]);

        $consumer->poll(10);
        $consumer->resume([self::TOPIC => [1]]);
        $consumer->poll(10);

        self::assertSame([0], array_keys($client->fetchCalls[0][self::TOPIC]), 'a paused partition is not fetched');
        self::assertSame(
            [1, 0],
            array_keys($client->fetchCalls[1][self::TOPIC]),
            'the partition that was served goes behind the one that was paused'
        );
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

    public function testSubscriptionIsEmptyUntilTopicsAreSubscribed(): void
    {
        $consumer = new TestKafkaConsumer(new FakeClient(), $this->configuration());

        self::assertSame([], $consumer->subscription());
        self::assertSame([], $consumer->assignment());
    }

    public function testSubscribeJoinsTheGroupOnTheFirstPollAndReceivesEveryPartition(): void
    {
        $client                     = $this->clientWithLog([0 => 2, 1 => 1, 2 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0, 1, 2]];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);

        self::assertSame([self::TOPIC], $consumer->subscription());
        self::assertSame([], $consumer->assignment(), 'nothing is sent to the broker before the first poll()');

        $records = $consumer->poll(10);

        self::assertSame([self::TOPIC => [0 => 0, 1 => 1, 2 => 2]], $consumer->assignment());
        self::assertCount(2, $records[self::TOPIC][0]);
        self::assertCount(1, $client->joins, 'one JoinGroup is enough to establish the membership');
        self::assertSame(self::GROUP, $client->joins[0]['groupId']);
        self::assertSame('', $client->joins[0]['memberId'], 'a client without a member id joins with an empty one');
        self::assertSame('consumer', $client->joins[0]['protocolType']);
        self::assertSame(['range'], array_keys($client->joins[0]['protocols']));
        self::assertEquals(
            new Subscription([self::TOPIC]),
            Subscription::unpack($client->joins[0]['protocols']['range']),
            'the member metadata of a consumer group is the Subscription of the member'
        );
        self::assertSame([self::TOPIC => [0, 1, 2]], $client->assignmentOf('member-1'));
    }

    public function testTheLeaderOfTheGroupAssignsThePartitionsOfEveryMember(): void
    {
        $client                     = $this->clientWithLog([0 => 1, 1 => 1, 2 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0, 1, 2]];
        $client->addGroupMember('member-9', [self::TOPIC]);
        $client->leaderId = 'member-1';

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        // `range` over one topic with three partitions and the members sorted lexicographically: 2 + 1
        self::assertSame([self::TOPIC => [0 => 0, 1 => 1]], $consumer->assignment());
        self::assertSame([self::TOPIC => [0, 1]], $client->assignmentOf('member-1'));
        self::assertSame([self::TOPIC => [2]], $client->assignmentOf('member-9'));
        self::assertSame(
            ['member-1', 'member-9'],
            array_keys($client->syncs[0]['assignments']),
            'the leader publishes an assignment for every member of the generation'
        );
    }

    public function testTheRoundRobinAssignorSpreadsThePartitionsOfTheLeaderDifferently(): void
    {
        $client                     = $this->clientWithLog([0 => 1, 1 => 1, 2 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0, 1, 2]];
        $client->addGroupMember('member-9', [self::TOPIC]);
        $client->leaderId = 'member-1';

        $consumer = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT            => false,
            ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => RoundRobinAssignor::NAME,
        ]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        self::assertSame(['roundrobin'], array_keys($client->joins[0]['protocols']));
        self::assertSame([self::TOPIC => [0, 2]], $client->assignmentOf('member-1'));
        self::assertSame([self::TOPIC => [1]], $client->assignmentOf('member-9'));
    }

    public function testAFollowerTakesTheAssignmentTheLeaderPublishedForIt(): void
    {
        $client                     = $this->clientWithLog([0 => 1, 1 => 1, 2 => 3]);
        $client->partitionsPerTopic = [self::TOPIC => [0, 1, 2]];
        $client->addGroupMember('member-9', [self::TOPIC]);
        $client->memberAssignments  = ['member-1' => new MemberAssignment([self::TOPIC => [2]])->pack()];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);
        $records = $consumer->poll(10);

        self::assertSame([self::TOPIC => [2 => 2]], $consumer->assignment());
        self::assertSame([], $client->syncs[0]['assignments'], 'a follower sends an empty SyncGroup');
        self::assertCount(3, $records[self::TOPIC][2]);
    }

    public function testAMemberThatGetsNoPartitionsPollsNothingAndStaysInTheGroup(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];
        $client->addGroupMember('member-9', [self::TOPIC]);

        $consumer = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT    => false,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS => 0,
        ]);
        $consumer->subscribe([self::TOPIC]);

        self::assertSame([], $consumer->poll(10), 'the leader left this member without partitions');
        self::assertSame([], $consumer->assignment());

        $consumer->poll(10);

        self::assertCount(1, $client->joins, 'a member without partitions does not rejoin on every poll');
        self::assertCount(1, $client->heartbeats, 'but it keeps its session alive with heartbeats');
    }

    public function testTheHeartbeatIsSentFromPollOnceTheIntervalHasElapsed(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT    => false,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS => 60000,
        ]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);
        $consumer->poll(10);

        self::assertSame([], $client->heartbeats, 'the interval has not elapsed since the join');

        $client->partitionsPerTopic = [self::TOPIC => [0]];
        $eager                      = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT    => false,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS => 0,
        ]);
        $eager->subscribe([self::TOPIC]);
        $eager->poll(10);
        $eager->poll(10);
        $eager->poll(10);

        self::assertCount(2, $client->heartbeats, 'at most one heartbeat per poll(), none on the joining one');
        self::assertSame('member-2', $client->heartbeats[0]['memberId']);
        self::assertSame(2, $client->heartbeats[0]['generationId']);
    }

    public function testARebalancingGroupIsRejoinedWithTheMemberIdOfThePreviousGeneration(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT    => false,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS => 0,
        ]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        $client->heartbeatFailures = [new RebalanceInProgressException(['groupId' => self::GROUP])];
        $consumer->poll(10);

        self::assertCount(2, $client->joins);
        self::assertSame('member-1', $client->joins[1]['memberId'], 'a member keeps its id across a rebalance');
        self::assertSame(2, $client->syncs[1]['generationId'], 'the rebalance produced the next generation');
        self::assertSame([self::TOPIC => [0 => 0]], $consumer->assignment());
    }

    public function testAMemberTheCoordinatorDroppedJoinsWithoutAMemberId(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT    => false,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS => 0,
        ]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        $client->heartbeatFailures = [new UnknownMemberIdException(['groupId' => self::GROUP])];
        $consumer->poll(10);

        self::assertCount(2, $client->joins);
        self::assertSame('', $client->joins[1]['memberId'], 'the member id of the dropped member is forgotten');
        self::assertSame('member-2', $client->syncs[1]['memberId'], 'the coordinator assigned a new one');
    }

    public function testAGenerationThatIsOverIsRejoinedOnTheNextPoll(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT    => false,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS => 0,
        ]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        $client->heartbeatFailures = [new IllegalGenerationException(['groupId' => self::GROUP])];
        $consumer->poll(10);

        self::assertCount(2, $client->joins);
        self::assertSame('member-1', $client->joins[1]['memberId']);
    }

    public function testARebalanceThatIsInterruptedByAnotherOneIsRepeated(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];
        $client->syncFailures       = [new RebalanceInProgressException(['groupId' => self::GROUP])];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        self::assertCount(2, $client->joins, 'the member joins again when its SyncGroup is answered with 27');
        self::assertSame([self::TOPIC => [0 => 0]], $consumer->assignment());
    }

    public function testARebalanceThatKeepsFailingIsReportedToTheApplication(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];
        $client->joinFailures       = array_fill(0, 5, new RebalanceInProgressException(['groupId' => self::GROUP]));

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);

        $this->expectException(RebalanceInProgressException::class);
        $consumer->poll(10);
    }

    public function testCommitsOfAGroupMemberCarryItsMemberIdAndGeneration(): void
    {
        $client                     = $this->clientWithLog([0 => 2]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        self::assertSame('member-1', $client->commits[0]['memberId']);
        self::assertSame(1, $client->commits[0]['generationId']);
        self::assertSame([self::TOPIC => [0 => 2]], $client->commits[0]['offsets']);
    }

    public function testCommitsOfAManuallyAssignedConsumerCarryNoMembership(): void
    {
        $client   = $this->clientWithLog([0 => 2]);
        $consumer = $this->consumer($client);
        $consumer->assign([self::TOPIC => [0]]);
        $consumer->poll(10);

        self::assertSame('', $client->commits[0]['memberId'], 'a simple consumer commits without a member id');
        self::assertSame(-1, $client->commits[0]['generationId']);
        self::assertSame([], $client->joins, 'assign() joins no group at all');
    }

    public function testThePositionsAreCommittedBeforeThePartitionsAreGivenUp(): void
    {
        $client                     = $this->clientWithLog([0 => 3]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [
            ConsumerConfig::AUTO_COMMIT_INTERVAL_MS => 60000,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS   => 0,
        ]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        self::assertCount(1, $client->commits, 'the very first poll() commits, there is no last commit to wait for');

        $client->heartbeatFailures = [new RebalanceInProgressException(['groupId' => self::GROUP])];
        $consumer->poll(10);

        self::assertCount(2, $client->commits, 'the interval did not elapse, this is the commit of the rebalance');
        self::assertSame(
            1,
            $client->commits[1]['generationId'],
            'the positions are committed with the generation that is being left, i.e. before the rejoin'
        );
        self::assertSame([self::TOPIC => [0 => 3]], $client->commits[1]['offsets']);
    }

    public function testACommitThatTheLostGenerationRefusesDoesNotStopTheRebalance(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [ConsumerConfig::HEARTBEAT_INTERVAL_MS => 0]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        $client->commitFailures    = [new IllegalGenerationException(['groupId' => self::GROUP])];
        $client->heartbeatFailures = [new RebalanceInProgressException(['groupId' => self::GROUP])];
        $consumer->poll(10);

        self::assertCount(2, $client->joins);
        self::assertSame([self::TOPIC => [0 => 0]], $consumer->assignment());
    }

    public function testTheRebalanceListenerSeesTheRevokedAndTheAssignedPartitions(): void
    {
        $client                     = $this->clientWithLog([0 => 1, 1 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0, 1]];
        $listener                   = new RecordingRebalanceListener();

        $consumer = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT    => false,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS => 0,
        ]);
        $consumer->subscribe([self::TOPIC], $listener);
        $consumer->poll(10);

        self::assertSame([['assigned', [self::TOPIC => [0, 1]]]], $listener->calls);

        $client->heartbeatFailures = [new RebalanceInProgressException(['groupId' => self::GROUP])];
        $consumer->poll(10);

        self::assertSame(
            [
                ['assigned', [self::TOPIC => [0, 1]]],
                ['revoked', [self::TOPIC => [0, 1]]],
                ['assigned', [self::TOPIC => [0, 1]]],
            ],
            $listener->calls,
            'a rebalance revokes the whole assignment before it hands the new one over'
        );
    }

    public function testUnsubscribeLeavesTheGroupAndForgetsTheMembership(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        $consumer->unsubscribe();

        self::assertSame([['groupId' => self::GROUP, 'memberId' => 'member-1']], $client->leaves);
        self::assertSame([], $consumer->assignment());
        self::assertSame([], $consumer->subscription());
        self::assertSame([], $consumer->poll(10), 'a consumer without a subscription fetches nothing');
        self::assertCount(1, $client->joins, 'and it does not join a group again on its own');
    }

    public function testCloseCommitsThePositionsAndLeavesTheGroup(): void
    {
        $client                     = $this->clientWithLog([0 => 2]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [ConsumerConfig::AUTO_COMMIT_INTERVAL_MS => 60000]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        $consumer->close();

        self::assertSame([self::TOPIC => [0 => 2]], $client->commits[0]['offsets']);
        self::assertSame('member-1', $client->commits[0]['memberId']);
        self::assertCount(1, $client->leaves);
        self::assertSame([], $consumer->subscription());
    }

    public function testAMemberThatLeavesAGroupItIsNotInAnyMoreIsNotAnError(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];
        $client->leaveFailures      = [new UnknownMemberIdException(['groupId' => self::GROUP])];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        $consumer->unsubscribe();

        self::assertSame([], $consumer->subscription());
    }

    public function testSubscribeAndAssignAreMutuallyExclusive(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);

        $this->expectException(InvalidArgumentException::class);
        $consumer->assign([self::TOPIC => [0]]);
    }

    public function testSubscribeOfAnEmptyListUnsubscribes(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);
        $consumer->subscribe([]);

        self::assertSame([], $consumer->subscription());
        self::assertCount(1, $client->leaves);
    }

    public function testSubscribeRefusesAnEmptyTopicName(): void
    {
        $consumer = $this->consumer(new FakeClient(), [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);

        $this->expectException(InvalidArgumentException::class);
        $consumer->subscribe([self::TOPIC, ' ']);
    }

    public function testSubscribeWithoutAGroupIsRefused(): void
    {
        $consumer = new TestKafkaConsumer(new FakeClient(), [
            ConsumerConfig::GROUP_ID           => '',
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessageMatches('/group\.id/');
        $consumer->subscribe([self::TOPIC]);
    }

    public function testSubscribeRefusesARequestTimeoutThatIsNotAboveTheSessionTimeout(): void
    {
        $consumer = $this->consumer(new FakeClient(), [
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
            ConsumerConfig::REQUEST_TIMEOUT_MS => 10000,
            ConsumerConfig::SESSION_TIMEOUT_MS => 10000,
        ]);

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessageMatches('/request\.timeout\.ms/');
        $consumer->subscribe([self::TOPIC]);
    }

    public function testSubscribeRefusesARequestTimeoutThatIsNotAboveTheMaxPollInterval(): void
    {
        // A JoinGroup blocks for up to the rebalance timeout, which is what max.poll.interval.ms is sent as
        $consumer = $this->consumer(new FakeClient(), [
            ConsumerConfig::ENABLE_AUTO_COMMIT   => false,
            ConsumerConfig::REQUEST_TIMEOUT_MS   => 40000,
            ConsumerConfig::SESSION_TIMEOUT_MS   => 10000,
            ConsumerConfig::MAX_POLL_INTERVAL_MS => 300000,
        ]);

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessageMatches('/max\.poll\.interval\.ms/');
        $consumer->subscribe([self::TOPIC]);
    }

    public function testTheDefaultConfigurationLetsAConsumerSubscribe(): void
    {
        $configuration = ConsumerConfig::getDefaultConfiguration();

        self::assertGreaterThan(
            $configuration[ConsumerConfig::SESSION_TIMEOUT_MS],
            $configuration[ConsumerConfig::REQUEST_TIMEOUT_MS],
            'a JoinGroup that waits for a whole rebalance must not run into the socket timeout'
        );
        self::assertGreaterThan(
            $configuration[ConsumerConfig::MAX_POLL_INTERVAL_MS],
            $configuration[ConsumerConfig::REQUEST_TIMEOUT_MS],
            'the coordinator holds a JoinGroup for a whole rebalance timeout, i.e. max.poll.interval.ms'
        );
        self::assertSame(300000, $configuration[ConsumerConfig::MAX_POLL_INTERVAL_MS], 'as in the Java consumer');
        self::assertSame(10000, $configuration[ConsumerConfig::SESSION_TIMEOUT_MS], 'as in the Java consumer');
    }

    public function testTheJoinGroupOfAPollCarriesTheConfiguredMaxPollInterval(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [
            ConsumerConfig::ENABLE_AUTO_COMMIT   => false,
            ConsumerConfig::MAX_POLL_INTERVAL_MS => 45000,
            ConsumerConfig::REQUEST_TIMEOUT_MS   => 50000,
        ]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        self::assertSame(
            45000,
            $client->joins[0]['rebalanceTimeout'],
            'the consumer sends max.poll.interval.ms as the rebalance_timeout of its JoinGroup v1'
        );
    }

    public function testAnAssignmentOfATopicThatWasNotSubscribedIsRefused(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];
        $client->addGroupMember('member-9', [self::TOPIC]);
        $client->memberAssignments  = ['member-1' => new MemberAssignment(['another-topic' => [0]])->pack()];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/another-topic/');
        $consumer->poll(10);
    }

    public function testTheSubscriptionStateIsAutoAssignedWhileSubscribed(): void
    {
        $client                     = $this->clientWithLog([0 => 1]);
        $client->partitionsPerTopic = [self::TOPIC => [0]];

        $consumer = $this->consumer($client, [ConsumerConfig::ENABLE_AUTO_COMMIT => false]);
        $consumer->subscribe([self::TOPIC]);
        $consumer->poll(10);

        self::assertSame([self::TOPIC], $consumer->subscription());
        self::assertSame(SubscriptionState::TYPE_AUTO_TOPICS, 1);
    }

    /**
     * Builds a fake client whose log holds the given number of records per partition
     *
     * @param array<int, int> $partitionRecordCounts Partition id => number of records in it
     * @param int             $firstOffset           Offset of the first record of every partition
     */
    /**
     * Builds a client whose only partition holds five records, one second apart, the way a timestamp lookup sees it
     */
    private function clientWithTimestampedLog(): FakeClient
    {
        $client = new FakeClient();
        $client->logStartOffsets[self::TOPIC][0] = 0;
        for ($offset = 0; $offset < 5; $offset++) {
            $client->log[self::TOPIC][0][] = new Record(
                'value-' . $offset,
                null,
                0,
                $offset,
                1600000000000 + $offset * 1000,
                TimestampType::CREATE_TIME
            );
        }

        return $client;
    }

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
