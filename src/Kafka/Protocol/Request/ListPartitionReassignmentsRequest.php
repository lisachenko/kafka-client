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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ListPartitionReassignmentsTopics;

/**
 * Asks the controller which partitions are being reassigned (ApiKey 46, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   ListPartitionReassignments Request (Version: 0) => TimeoutMs Topics TAG_BUFFER
 *     TimeoutMs => INT32
 *     Topics    => COMPACT_NULLABLE_ARRAY of {@see ListPartitionReassignmentsTopics}
 * </pre>
 *
 * The second half of KIP-455: {@see AlterPartitionReassignmentsRequest} (key 45) submits a reassignment, this api
 * says what is still running. Before Kafka 2.4 the answer was the content of the `/admin/reassign_partitions`
 * znode, which is why `kafka-reassign-partitions.sh --verify` had to talk to ZooKeeper.
 *
 * **The topic array is nullable and the two empty values differ**: `null` asks for every reassignment of the
 * cluster, an empty array asks for nothing at all. A client of a shared cluster should name its topics, because the
 * null array answers the reassignments of everybody.
 *
 * Like key 45 the api is **flexible from its version 0**, and only the **active controller** serves it; a broker
 * that is not the controller answers the top-level error code 41 (`NotController`).
 *
 * @see docs/protocol/2.8.md, section "ListPartitionReassignments API (key 46, v0)"
 */
class ListPartitionReassignmentsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::LIST_PARTITION_REASSIGNMENTS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Default of the `TimeoutMs` field of the specification, one minute
     */
    public const int DEFAULT_TIMEOUT_MS = 60000;

    /**
     * Topics to ask for, indexed by the topic name, or null for every reassignment of the cluster
     *
     * @var array<string, ListPartitionReassignmentsTopics>|null
     */
    protected ?array $topics;

    /**
     * @param array<string, list<int>>|null $partitions Partitions to ask for, as `topic => [partition indexes]`,
     *        or `null` for every reassignment the cluster has in progress
     * @param int    $timeoutMs     How long the controller waits before it answers
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        ?array $partitions = null,
        protected int $timeoutMs = self::DEFAULT_TIMEOUT_MS,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $topics = null;
        if ($partitions !== null) {
            $topics = [];
            foreach ($partitions as $topic => $partitionIndexes) {
                $topics[(string) $topic] = new ListPartitionReassignmentsTopics(
                    (string) $topic,
                    array_map(intval(...), $partitionIndexes)
                );
            }
        }
        $this->topics = $topics;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'timeoutMs' => BinarySchema::TYPE_INT32,
            'topics'    => [
                'name' => ListPartitionReassignmentsTopics::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
        ];
    }

    /**
     * Returns the timeout the request states, in milliseconds
     */
    public function getTimeoutMs(): int
    {
        return $this->timeoutMs;
    }
}
