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
use Protocol\Kafka\Protocol\Data\CreateTopicsRequestTopic;

/**
 * CreateTopics, version 0: the request of Kafka 0.10.1, without the `validate_only` flag
 *
 * <pre>
 *   CreateTopics Request (Version: 0) => [create_topic_requests] timeout
 *     create_topic_requests => topic num_partitions replication_factor [replica_assignment] [configs]
 *     timeout               => INT32
 * </pre>
 *
 * The topic entries are the ones of version 1 (`SINGLE_CREATE_TOPIC_REQUEST_V1 = SINGLE_CREATE_TOPIC_REQUEST_V0` in
 * `Protocol.java` @ 0.10.2.2); only the trailing boolean is missing, and the answer to this version carries no
 * `ErrorMessage` either, see {@see CreateTopicsResponseV0}. A 0.10.2.2 broker still serves it, which is what
 * `tests/Integration/TopicAdminApiTest.php` checks.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v5)"
 */
final class CreateTopicsRequestV0 extends CreateTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param array<int|string, NewTopic|CreateTopicsRequestTopic> $topics Topics to create
     * @param int    $timeout       How long the controller waits for the topics to be created, in milliseconds
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $topics,
        int $timeout = 30000,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct($topics, $timeout, false, $clientId, $correlationId);
    }
}
