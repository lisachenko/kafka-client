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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OngoingTopicReassignment;

/**
 * The partition reassignments a cluster has in progress (ApiKey 46, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   ListPartitionReassignments Response (Version: 0) => ThrottleTimeMs ErrorCode ErrorMessage Topics TAG_BUFFER
 *     ThrottleTimeMs => INT32
 *     ErrorCode      => INT16
 *     ErrorMessage   => COMPACT_NULLABLE_STRING
 *     Topics         => COMPACT_ARRAY of {@see OngoingTopicReassignment}
 * </pre>
 *
 * There is **one** error code here, for the whole request: 41 (`NotController`) when the request did not reach the
 * controller, 31 (`ClusterAuthorizationFailed`) without the `DESCRIBE` permission on the cluster, 0 otherwise. A
 * topic or a partition that does not exist is not an error - it is simply not in the answer, because the answer is
 * what is *going on* and not what was asked for.
 *
 * **The empty answer is the normal one.** A reassignment only shows up while it is in progress, and on the
 * one-broker container of this repository it never is: every replica is already on the only broker, so the
 * controller completes a reassignment before it answers the request that submitted it. The wire vectors of this api
 * are therefore the empty answer of a request that named a real topic, and the shape of a partition in flight is
 * documented from the sources in {@see \Protocol\Kafka\Protocol\Data\OngoingPartitionReassignment}.
 *
 * @see docs/protocol/2.8.md, section "ListPartitionReassignments API (key 46, v0)"
 */
class ListPartitionReassignmentsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the whole request, 0 when it reached the controller
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Topics with a reassignment in progress, indexed by the topic name
     *
     * @var array<string, OngoingTopicReassignment>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
            'topics'         => ['name' => OngoingTopicReassignment::class],
        ];
    }
}
