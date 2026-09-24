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
use Protocol\Kafka\Protocol\Data\ShareAcknowledgeResponsePartition;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgeResponseTopic;
use Protocol\Kafka\Protocol\Data\ShareNodeEndpoint;

/**
 * ShareAcknowledge response, version 1 (key 79, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   ShareAcknowledge Response (Version: 1) => throttle_time_ms error_code error_message [responses] [node_endpoints]
 *     responses => topic_id [partitions]
 *       partitions => partition_index error_code error_message current_leader
 * </pre>
 *
 * The top-level code is the one of the share session (122, 123) and of the request (42, 30); every partition answers
 * its acknowledgements on its own, the **121** `InvalidRecordState` of an offset the member does not hold among them.
 *
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1)"
 */
class ShareAcknowledgeResponse extends AbstractResponse
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
     * Topics of the answer
     *
     * @var list<ShareAcknowledgeResponseTopic>
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
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
            'responses'      => [ShareAcknowledgeResponseTopic::class],
            'nodeEndpoints'  => [ShareNodeEndpoint::class],
        ];
    }

    /**
     * Returns the answer of one partition, null when the answer does not name it
     *
     * @param string $topicId The 16 raw bytes of the topic id
     */
    public function partitionOf(string $topicId, int $partition): ?ShareAcknowledgeResponsePartition
    {
        foreach ($this->responses as $topic) {
            if ($topic->topicId === $topicId && isset($topic->partitions[$partition])) {
                return $topic->partitions[$partition];
            }
        }

        return null;
    }
}
