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
use Protocol\Kafka\Protocol\Data\ReassignableTopicResponse;

/**
 * What the controller made of a partition reassignment (ApiKey 45, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   AlterPartitionReassignments Response (Version: 0) => ThrottleTimeMs ErrorCode ErrorMessage Responses TAG_BUFFER
 *     ThrottleTimeMs => INT32
 *     ErrorCode      => INT16
 *     ErrorMessage   => COMPACT_NULLABLE_STRING
 *     Responses      => COMPACT_ARRAY of {@see ReassignableTopicResponse}
 * </pre>
 *
 * **The answer carries two levels of error and they mean different things.** The top-level pair belongs to the
 * request as a whole - 41 `NotController` for a broker that is not the controller, 31
 * `ClusterAuthorizationFailed` without the `ALTER` permission on the cluster - and it stays **0** when the
 * request reached the right broker, whatever happened to the individual partitions. Every partition then carries
 * its own code: 0, 3, 39 or 85, see {@see \Protocol\Kafka\Protocol\Data\ReassignablePartitionResponse}.
 *
 * A partition that is answered with the code 0 is one the controller **accepted**, not one that has been moved: the
 * data travels afterwards, and a reassignment that was already satisfied - a target replica set the partition
 * already has - is answered with 0 as well, because there is nothing left to do.
 *
 * @see docs/protocol/2.8.md, section "AlterPartitionReassignments API (key 45, v0)"
 */
class AlterPartitionReassignmentsResponse extends AbstractResponse
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
     * Error code of the whole request, 0 when it reached the controller at all
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the top-level error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Result of every requested topic, indexed by the topic name
     *
     * @var array<string, ReassignableTopicResponse>
     */
    public array $responses = [];

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
            'responses'      => ['name' => ReassignableTopicResponse::class],
        ];
    }
}
