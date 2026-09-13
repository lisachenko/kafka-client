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
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Consumer\Internals\SubscriptionState;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * Verifies the bookkeeping of the assignment, the positions and the paused partitions of a consumer
 */
#[CoversClass(SubscriptionState::class)]
final class SubscriptionStateTest extends TestCase
{
    private const string TOPIC = 't9-subscription-state';

    public function testFreshStateHasNoAssignmentAndNoSubscription(): void
    {
        $state = new SubscriptionState();

        self::assertSame(SubscriptionState::TYPE_NONE, $state->getSubscriptionType());
        self::assertSame([], $state->getAssignment());
        self::assertSame([], $state->getSubscription());
        self::assertSame([], $state->fetchablePartitions());
        self::assertSame([], $state->allConsumed());
        self::assertFalse($state->isAssigned(self::TOPIC, 0));
    }

    public function testUserAssignmentStartsWithoutAKnownPosition(): void
    {
        $state = $this->assignedState([0, 1]);

        self::assertSame(SubscriptionState::TYPE_USER_ASSIGNED, $state->getSubscriptionType());
        self::assertTrue($state->isAssigned(self::TOPIC, 0));
        self::assertTrue($state->isAssigned(self::TOPIC, 1));
        self::assertFalse($state->isAssigned(self::TOPIC, 2));

        // A partition whose position is unknown is neither fetchable nor committable
        self::assertSame([], $state->fetchablePartitions());
        self::assertSame([], $state->allConsumed());
    }

    public function testSeekDefinesThePositionOfAPartition(): void
    {
        $state = $this->assignedState([0, 1]);
        $state->seek(self::TOPIC, 0, 42);

        self::assertSame(42, $state->position(self::TOPIC, 0));
        self::assertSame([self::TOPIC => [0 => 42]], $state->fetchablePartitions());
        self::assertSame([self::TOPIC => [0 => 42]], $state->allConsumed());
    }

    public function testPositionOfAPartitionWithoutOneIsRejected(): void
    {
        $state = $this->assignedState([0]);

        $this->expectException(UnknownTopicOrPartitionException::class);
        $state->position(self::TOPIC, 0);
    }

    public function testSeekOfANotAssignedPartitionIsRejected(): void
    {
        $state = $this->assignedState([0]);

        $this->expectException(UnknownTopicOrPartitionException::class);
        $state->seek(self::TOPIC, 7, 1);
    }

    public function testPositionOfANotAssignedPartitionIsRejected(): void
    {
        $state = $this->assignedState([0]);

        $this->expectException(UnknownTopicOrPartitionException::class);
        $state->position('another-topic', 0);
    }

    public function testPausedPartitionKeepsItsPositionButIsNotFetched(): void
    {
        $state = $this->assignedState([0, 1]);
        $state->seek(self::TOPIC, 0, 10);
        $state->seek(self::TOPIC, 1, 20);

        $state->pause([self::TOPIC => [1]]);

        self::assertTrue($state->isPaused(self::TOPIC, 1));
        self::assertSame([self::TOPIC => [0 => 10]], $state->fetchablePartitions());
        self::assertSame([self::TOPIC => [0 => 10, 1 => 20]], $state->allConsumed(), 'a paused partition is committed');
        self::assertSame(20, $state->position(self::TOPIC, 1));

        $state->resume([self::TOPIC => [1]]);

        self::assertFalse($state->isPaused(self::TOPIC, 1));
        self::assertSame([self::TOPIC => [0 => 10, 1 => 20]], $state->fetchablePartitions());
    }

    public function testPauseAddressesThePartitionsByValueAndNotByIndex(): void
    {
        $state = $this->assignedState([3, 4]);
        $state->seek(self::TOPIC, 3, 1);
        $state->seek(self::TOPIC, 4, 2);

        $state->pause([self::TOPIC => [4]]);

        self::assertFalse($state->isPaused(self::TOPIC, 3));
        self::assertTrue($state->isPaused(self::TOPIC, 4));
    }

    public function testPauseOfANotAssignedPartitionIsRejected(): void
    {
        $state = $this->assignedState([0]);

        $this->expectException(InvalidArgumentException::class);
        $state->pause([self::TOPIC => [9]]);
    }

    public function testReassignmentKeepsThePositionOfThePartitionsThatStay(): void
    {
        $state = $this->assignedState([0, 1]);
        $state->seek(self::TOPIC, 0, 10);
        $state->seek(self::TOPIC, 1, 20);
        $state->pause([self::TOPIC => [0]]);

        $state->assignFromUser([self::TOPIC => new PartitionsForTopic(self::TOPIC, [0, 2])]);

        self::assertSame([0, 2], array_keys($state->getAssignment()[self::TOPIC]));
        self::assertSame(10, $state->position(self::TOPIC, 0));
        self::assertTrue($state->isPaused(self::TOPIC, 0), 'a partition that stays assigned stays paused');
        self::assertSame([], $state->fetchablePartitions(), 'the new partition has no position yet');
    }

    public function testUnsubscribeDropsEverything(): void
    {
        $state = $this->assignedState([0]);
        $state->seek(self::TOPIC, 0, 5);

        $state->unsubscribe();

        self::assertSame(SubscriptionState::TYPE_NONE, $state->getSubscriptionType());
        self::assertSame([], $state->getAssignment());
        self::assertSame([], $state->allConsumed());
    }

    public function testSubscriptionByTopicsHasNoPartitionsUntilTheGroupAssignsThem(): void
    {
        $state = new SubscriptionState();
        $state->subscribeByTopics([self::TOPIC, 'another-topic']);

        self::assertSame(SubscriptionState::TYPE_AUTO_TOPICS, $state->getSubscriptionType());
        self::assertTrue($state->partitionsAutoAssigned());
        self::assertSame([self::TOPIC, 'another-topic'], $state->getSubscription());
        self::assertSame([], $state->getAssignment());
    }

    public function testAssignmentFromTheGroupIsStoredWithUnknownPositions(): void
    {
        $state = new SubscriptionState();
        $state->subscribeByTopics([self::TOPIC]);
        $state->assignFromSubscribed([self::TOPIC => new PartitionsForTopic(self::TOPIC, [1, 2])]);

        self::assertTrue($state->isAssigned(self::TOPIC, 1));
        self::assertTrue($state->isAssigned(self::TOPIC, 2));
        self::assertSame([], $state->allConsumed(), 'the positions are read from the committed offsets afterwards');

        $state->seek(self::TOPIC, 1, 7);

        self::assertSame([self::TOPIC => [1 => 7]], $state->fetchablePartitions());
    }

    public function testAnAssignmentOfANotSubscribedTopicIsRejected(): void
    {
        $state = new SubscriptionState();
        $state->subscribeByTopics([self::TOPIC]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/another-topic/');
        $state->assignFromSubscribed(['another-topic' => new PartitionsForTopic('another-topic', [0])]);
    }

    public function testAGroupAssignmentIsRejectedForAManuallyAssignedConsumer(): void
    {
        $state = $this->assignedState([0]);

        self::assertFalse($state->partitionsAutoAssigned());

        $this->expectException(InvalidArgumentException::class);
        $state->assignFromSubscribed([self::TOPIC => new PartitionsForTopic(self::TOPIC, [0])]);
    }

    public function testASubscriptionAndAManualAssignmentAreMutuallyExclusive(): void
    {
        $state = new SubscriptionState();
        $state->subscribeByTopics([self::TOPIC]);

        $this->expectException(InvalidArgumentException::class);
        $state->assignFromUser([self::TOPIC => new PartitionsForTopic(self::TOPIC, [0])]);
    }

    public function testUnsubscribeDropsTheSubscriptionAsWell(): void
    {
        $state = new SubscriptionState();
        $state->subscribeByTopics([self::TOPIC]);
        $state->assignFromSubscribed([self::TOPIC => new PartitionsForTopic(self::TOPIC, [0])]);

        $state->unsubscribe();

        self::assertSame([], $state->getSubscription());
        self::assertFalse($state->partitionsAutoAssigned());
        self::assertSame(SubscriptionState::TYPE_NONE, $state->getSubscriptionType());
    }

    public function testAPositionCarriesTheLeaderEpochItWasTakenAt(): void
    {
        $state = $this->assignedState([0]);

        self::assertNull($state->positionEpoch(self::TOPIC, 0), 'a fresh assignment knows no epoch');
        self::assertNull($state->currentLeaderEpoch(self::TOPIC, 0));
        self::assertFalse($state->needsValidation(self::TOPIC, 0));

        $state->seek(self::TOPIC, 0, 42, 3);

        self::assertSame(42, $state->position(self::TOPIC, 0));
        self::assertSame(3, $state->positionEpoch(self::TOPIC, 0), 'the epoch of KIP-320 travels with the offset');

        // A seek without an epoch is "I do not know where this offset comes from" and drops the old one
        $state->seek(self::TOPIC, 0, 50);

        self::assertNull($state->positionEpoch(self::TOPIC, 0));
    }

    public function testOnlyAMovedLeaderEpochMarksThePositionForValidation(): void
    {
        $state = $this->assignedState([0]);
        $state->seek(self::TOPIC, 0, 42, 3);

        // The first epoch the metadata reports is not a leader change, it is the first thing this client knows
        $state->setCurrentLeaderEpoch(self::TOPIC, 0, 3);
        self::assertSame(3, $state->currentLeaderEpoch(self::TOPIC, 0));
        self::assertFalse($state->needsValidation(self::TOPIC, 0));

        $state->setCurrentLeaderEpoch(self::TOPIC, 0, 3);
        self::assertFalse($state->needsValidation(self::TOPIC, 0), 'the same epoch again changes nothing');

        $state->setCurrentLeaderEpoch(self::TOPIC, 0, 4);
        self::assertTrue($state->needsValidation(self::TOPIC, 0), 'a new leader means the position has to be checked');

        $state->completeValidation(self::TOPIC, 0);
        self::assertFalse($state->needsValidation(self::TOPIC, 0));
        self::assertSame(4, $state->currentLeaderEpoch(self::TOPIC, 0));

        // An unknown epoch - a Metadata answer below version 7 - never marks anything
        $state->setCurrentLeaderEpoch(self::TOPIC, 0, null);
        self::assertFalse($state->needsValidation(self::TOPIC, 0));
    }

    /**
     * @param list<int> $partitions
     */
    private function assignedState(array $partitions): SubscriptionState
    {
        $state = new SubscriptionState();
        $state->assignFromUser([self::TOPIC => new PartitionsForTopic(self::TOPIC, $partitions)]);

        return $state;
    }
}
