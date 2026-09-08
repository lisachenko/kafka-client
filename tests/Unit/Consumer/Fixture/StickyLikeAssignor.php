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

namespace Protocol\Kafka\Tests\Unit\Consumer\Fixture;

use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\PartitionAssignorInterface;
use Protocol\Kafka\Consumer\Subscription;

/**
 * A custom assignor that a user of the library could write: it implements the interface directly and uses the
 * `userData` of the subscriptions, the way a sticky assignor carries the assignment of the previous generation.
 *
 * Every member announces the partition it held before as `keep-<partition>` and gets exactly that one back.
 */
final class StickyLikeAssignor implements PartitionAssignorInterface
{
    /**
     * @inheritdoc
     */
    public function name(): string
    {
        return 'sticky-like';
    }

    /**
     * @inheritdoc
     */
    public function subscription(array $topics): Subscription
    {
        return new Subscription($topics, userData: 'previous-generation');
    }

    /**
     * @inheritdoc
     */
    public function assign(array $partitionsPerTopic, array $subscriptions): array
    {
        $assignments = [];
        foreach ($subscriptions as $memberId => $subscription) {
            $held        = (int) substr((string) $subscription->userData, strlen('keep-'));
            $memberTopics = [];
            foreach ($subscription->topics as $topic) {
                $memberTopics[$topic] = [$held];
            }
            $assignments[$memberId] = new MemberAssignment($memberTopics);
        }

        return $assignments;
    }
}
