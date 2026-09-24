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

namespace Protocol\Kafka\Protocol\Request;

/**
 * ConsumerGroupHeartbeat request of version 0 (Kafka 3.5, KIP-848): the frame without a regex subscription
 *
 * Version 1 (Kafka 4.0) put the nullable `subscribed_topic_regex` behind `subscribed_topic_names` and made the member
 * id the consumer's own for every frame (KIP-1082); {@see ConsumerGroupHeartbeatRequest} sends it. This class writes
 * the frame of version 0, which has no field for a regex: a `$subscribedTopicRegex` given to it is not sent. A 4.3.1
 * node still serves the version, and still answers a version 0 join without a member id with one it generated.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupHeartbeat API (key 68, v0 and v1)"
 */
final class ConsumerGroupHeartbeatRequestV0 extends ConsumerGroupHeartbeatRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
