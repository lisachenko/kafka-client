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
use Protocol\Kafka\Protocol\Data\ShareFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\ShareFetchResponseTopic;
use Protocol\Kafka\Protocol\Data\ShareNodeEndpoint;

/**
 * ShareFetch response, version 1 (key 78, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   ShareFetch Response (Version: 1) => throttle_time_ms error_code error_message acquisition_lock_timeout_ms
 *                                       [responses] [node_endpoints]
 *     acquisition_lock_timeout_ms => INT32             -- since version 1
 *     responses      => topic_id [partitions]
 *     node_endpoints => node_id host port rack
 * </pre>
 *
 * `ShareFetchResponse.json` @ 4.1.0. The **top-level** error code is the one of the share session - 122, 123, 133 -
 * and of the request as a whole (the 42 of a null group or of an invalid member id, the 30 of a principal that may
 * not `READ` the group); a partition carries the rest ({@see ShareFetchResponsePartition}).
 * `acquisition_lock_timeout_ms` is how long this member holds the records it acquired, `share.record.lock.duration.ms`
 * of the group (30000 by default): a record neither accepted, released nor rejected by then is released by the
 * broker and delivered again.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 */
class ShareFetchResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * The api is flexible from its first version
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Top-level error code
     */
    public int $errorCode = 0;

    /**
     * Top-level error message, null without an error
     */
    public ?string $errorMessage = null;

    /**
     * How long the acquired records stay locked for this member, in milliseconds
     *
     * @since Version 1 of protocol
     */
    public int $acquisitionLockTimeoutMs = 0;

    /**
     * Topics of the answer, in the order the broker wrote them
     *
     * @var list<ShareFetchResponseTopic>
     */
    public array $responses = [];

    /**
     * Endpoints of the leaders the partitions of a 6 name
     *
     * @var list<ShareNodeEndpoint>
     */
    public array $nodeEndpoints = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return parent::getScheme() + [
            'throttleTimeMs'           => BinarySchema::TYPE_INT32,
            'errorCode'                => BinarySchema::TYPE_INT16,
            'errorMessage'             => BinarySchema::TYPE_NULLABLE_STRING,
            'acquisitionLockTimeoutMs' => BinarySchema::TYPE_INT32,
            'responses'                => [ShareFetchResponseTopic::class],
            'nodeEndpoints'            => [ShareNodeEndpoint::class],
        ];
    }

    /**
     * Returns the answer of one partition, null when the answer does not name it
     *
     * @param string $topicId The 16 raw bytes of the topic id
     */
    public function partitionOf(string $topicId, int $partition): ?ShareFetchResponsePartition
    {
        foreach ($this->responses as $topic) {
            if ($topic->topicId === $topicId && isset($topic->partitions[$partition])) {
                return $topic->partitions[$partition];
            }
        }

        return null;
    }
}
