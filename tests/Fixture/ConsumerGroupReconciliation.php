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

namespace Protocol\Kafka\Tests\Fixture;

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\ConsumerGroupDescription;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Node;
use RuntimeException;

/**
 * Drives the members of a KIP-848 group by hand until the group is settled.
 *
 * Kafka 4.3 moved the target assignment of a consumer group off the heartbeat path: the coordinator recomputes it
 * at most once per `group.consumer.assignment.interval.ms` (1000 ms by default,
 * `GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT` @ 4.3.0) and, with
 * `group.consumer.assignor.offload.enable`, on a background thread. A heartbeat that follows a join therefore no
 * longer carries the new assignment of its member for certain - the group is `Assigning` until the computation
 * lands - and a test that expected the revoke in the very next frame, as it did on the 3.9.2 node of the 3.x line,
 * has to heartbeat until it arrives.
 *
 * {@see self::settle()} heartbeats every member in turn, acknowledges every assignment a member is handed with the
 * heartbeat after it, and stops once ConsumerGroupDescribe reports the group `Stable` with every member reconciled.
 */
final class ConsumerGroupReconciliation
{
    /**
     * Pause between two rounds of heartbeats, in microseconds
     */
    private const int ROUND_PAUSE_US = 200000;

    public function __construct(
        private readonly Client $client,
        private readonly AdminClient $admin,
        private readonly Node $coordinator,
        private readonly string $groupId,
        private readonly float $timeoutSeconds = 30.0
    ) {}

    /**
     * Heartbeats the members until the group is settled, and answers its description
     *
     * @param array<string, int>                      $epochs Member id => epoch, updated in place
     * @param array<string, array<string, list<int>>> $owned  Member id => raw topic id => partitions the member
     *        owns, updated in place
     */
    public function settle(array &$epochs, array &$owned): ConsumerGroupDescription
    {
        $deadline = microtime(true) + $this->timeoutSeconds;

        while (true) {
            foreach (array_keys($epochs) as $memberId) {
                $memberId = (string) $memberId;
                $answer   = $this->client->consumerGroupHeartbeat(
                    $this->coordinator,
                    $this->groupId,
                    $memberId,
                    $epochs[$memberId]
                );
                $epochs[$memberId] = $answer->memberEpoch;
                if ($answer->assignment === null) {
                    continue;
                }

                // A changed assignment is acknowledged by the next heartbeat, which echoes what the member owns now
                $owned[$memberId] = $answer->assignment->partitionsByTopicId();
                $acknowledged     = $this->client->consumerGroupHeartbeat(
                    $this->coordinator,
                    $this->groupId,
                    $memberId,
                    $epochs[$memberId],
                    null,
                    $owned[$memberId]
                );
                $epochs[$memberId] = $acknowledged->memberEpoch;
                if ($acknowledged->assignment !== null) {
                    $owned[$memberId] = $acknowledged->assignment->partitionsByTopicId();
                }
            }

            $description = $this->admin->describeConsumerGroup($this->groupId);
            if (self::isSettled($description, array_keys($epochs))) {
                return $description;
            }
            if (microtime(true) >= $deadline) {
                throw new RuntimeException(sprintf(
                    'The group %s did not settle within %.0f s: state %s, group epoch %d, assignment epoch %d',
                    $this->groupId,
                    $this->timeoutSeconds,
                    $description->state,
                    $description->groupEpoch,
                    $description->assignmentEpoch
                ));
            }
            usleep(self::ROUND_PAUSE_US);
        }
    }

    /**
     * Tells whether the group is stable, computed for its current epoch and every given member reconciled
     *
     * @param list<string|int> $memberIds
     */
    private static function isSettled(ConsumerGroupDescription $description, array $memberIds): bool
    {
        if (!$description->isStable() || $description->groupEpoch !== $description->assignmentEpoch) {
            return false;
        }
        foreach ($memberIds as $memberId) {
            $member = $description->members[(string) $memberId] ?? null;
            if ($member === null || !$member->isReconciled() || $member->memberEpoch !== $description->groupEpoch) {
                return false;
            }
        }

        return true;
    }
}
