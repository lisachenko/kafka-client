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

use Protocol\Kafka\Admin\NewTopic;

/**
 * CreateTopics request of version 3 (Kafka 2.0), the frame of version 4 with a lower version field
 *
 * <pre>
 *   CreateTopics Request (Version: 1, 2 and 3) => [create_topic_requests] timeout validate_only
 * </pre>
 *
 * Kafka 2.4 raised the api to version 4 for **KIP-464** and changed not one byte of the layout: what the version
 * buys is the permission to leave `num_partitions` and `replication_factor` at **-1** without an explicit replica
 * assignment, which asks the broker for its own `num.partitions` and `default.replication.factor`. A version 3
 * request of that shape is refused by the CLIENT: `CreateTopicsRequest.Builder.build(version)` @ 2.8.2 throws an
 * `UnsupportedVersionException` for it, and so does {@see CreateTopicsRequest::__construct()}. The check has to sit
 * there, because a 2.8.2 broker does not refuse the bytes - `ZkAdminManager.createTopics` @ 2.8.2 resolves the -1
 * against `num.partitions` and `default.replication.factor` whatever version it was asked with, and it is the
 * broker of Kafka 2.3 and below that has no such fallback and answers 37 or 38 instead. This class is therefore
 * what a client sends to a broker below Kafka 2.4, with both numbers named or with an assignment of its own
 * ({@see NewTopic::withReplicaAssignment()}).
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v6)"
 */
final class CreateTopicsRequestV3 extends CreateTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
