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

use Protocol\Kafka\Admin\NewPartitionReassignment;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ReassignablePartition;
use Protocol\Kafka\Protocol\Data\ReassignableTopic;

/**
 * Moves the replicas of partitions to other brokers, or cancels a move (ApiKey 45, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   AlterPartitionReassignments Request (Version: 0) => TimeoutMs Topics TAG_BUFFER
 *     TimeoutMs => INT32
 *     Topics    => COMPACT_ARRAY of {@see ReassignableTopic}
 * </pre>
 *
 * KIP-455 is the api that took the last piece of `kafka-reassign-partitions.sh` away from ZooKeeper. Before Kafka
 * 2.4 a reassignment was a JSON document written into the `/admin/reassign_partitions` znode: only one could be in
 * flight for the whole cluster, it could not be cancelled, and nothing but the znode said whether it was still
 * running. The api replaces all three - reassignments are submitted per partition, they can be cancelled one by
 * one, and {@see ListPartitionReassignmentsRequest} (key 46) reports what is in progress.
 *
 * **The api is flexible from its version 0**: it was added by the release that introduced KIP-482, so there is no
 * pre-flexible version of it to be compatible with - the request header is the v2, every string and array is
 * compact, and every structure ends in a tagged-field section.
 *
 * **Only the active controller serves it.** A broker that is not (or is no longer) the controller answers the
 * top-level error code 41 (`NotController`), so the request goes to
 * {@see \Protocol\Kafka\Admin\AdminClient::findController()} exactly like CreateTopics and CreatePartitions.
 *
 * `TimeoutMs` is how long the controller waits for the reassignment to be *registered*, not for it to finish: the
 * api answers as soon as the controller has written the new target assignment, and the data is moved afterwards by
 * the replica fetchers. The work is watched with key 46, and it is throttled by the broker options
 * `leader.replication.throttled.rate`/`follower.replication.throttled.rate`, which this api does not touch.
 *
 * @see docs/protocol/2.8.md, section "AlterPartitionReassignments API (key 45, v0)"
 */
class AlterPartitionReassignmentsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ALTER_PARTITION_REASSIGNMENTS;

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
     * Topics whose partitions are reassigned, indexed by the topic name
     *
     * @var array<string, ReassignableTopic>
     */
    protected array $topics;

    /**
     * @param array<string, array<int, list<int>|NewPartitionReassignment|null>> $reassignments Target replica set of
     *        every partition, as `topic => [partition => [broker ids]]`; `null` cancels the reassignment of that
     *        partition
     * @param int    $timeoutMs     How long the controller waits before it answers
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $reassignments,
        protected int $timeoutMs = self::DEFAULT_TIMEOUT_MS,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $topics = [];
        foreach ($reassignments as $topic => $partitions) {
            $entries = [];
            foreach ($partitions as $partition => $replicas) {
                if ($replicas instanceof NewPartitionReassignment) {
                    $replicas = $replicas->targetReplicas;
                }
                $entries[$partition] = new ReassignablePartition((int) $partition, $replicas);
            }
            $topics[(string) $topic] = new ReassignableTopic((string) $topic, $entries);
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
            'topics'    => ['name' => ReassignableTopic::class],
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
