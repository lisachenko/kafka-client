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

use Protocol\Kafka\Admin\ElectionType;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ElectLeadersRequestTopicPartitions;

/**
 * ElectLeaders, version 0: asks the controller to elect the preferred leader of partitions (ApiKey 43, Kafka 2.2)
 *
 * <pre>
 *   ElectLeaders Request (Version: 0) => [topic_partitions] timeout_ms
 *     topic_partitions => topic [partition_id]      -- NULLABLE: null means every partition of the cluster
 *       topic        => STRING
 *       partition_id => INT32
 *     timeout_ms => INT32
 * </pre>
 *
 * KIP-183 added the api in Kafka 2.2 under the name **ElectPreferredLeaders**: it does through the protocol what
 * `kafka-preferred-replica-election.sh` did by writing the ZooKeeper node `/admin/preferred_replica_election`,
 * i.e. it moves the leadership of a partition back to the **preferred replica** - the first broker of the
 * assignment - when that replica is in the ISR. Kafka 2.4 renamed the api to ElectLeaders and gave its version 1
 * a leading `election_type` byte for the unclean election of KIP-460; version 0 has no such field and can only ask
 * for the preferred one, which is why {@see \Protocol\Kafka\Admin\AdminClient::electLeaders()} refuses
 * {@see ElectionType::UNCLEAN} on this line.
 *
 * **Only the active controller serves it** (`KafkaApis.handleElectReplicaLeader` @ 2.8.2 requires `zkSupport` and
 * hands the partitions to `ReplicaManager.electLeaders`, which asks the controller); a broker that is not the
 * controller answers **41** `NotController`. The container of this line runs a single broker, which is therefore
 * always the controller.
 *
 * `topic_partitions` is **nullable**, and the two shapes mean different things:
 *
 *  - a **null** array asks the controller to look at *every* partition it knows - `metadataCache.getAllPartitions()`
 *    - and the answer then leaves out every partition whose leader was already the right one ("partitions that
 *    didn't need election because they ready have the correct leader are not returned to the client");
 *  - a **named** array asks for those partitions alone, and every one of them gets an entry in the answer,
 *    including the **84** `ElectionNotNeeded` of a partition that is already led by its preferred replica.
 *
 * `timeout_ms` is how long the controller waits for the elections to finish before it answers; its default in the
 * Java client is 60 seconds, which is what {@see self::DEFAULT_TIMEOUT_MS} carries.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0)"
 */
class ElectLeadersRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ELECT_LEADERS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Timeout of the Java client, `ElectLeadersRequest.json` @ 2.8.2: `"default": "60000"`
     */
    public const int DEFAULT_TIMEOUT_MS = 60000;

    /**
     * `topic_partitions` of a request that asks the controller to look at every partition of the cluster
     */
    public const ?array ALL_PARTITIONS = null;

    /**
     * Partitions whose leader should be elected, indexed by the topic name, or null for every partition
     *
     * @var array<string, ElectLeadersRequestTopicPartitions>|null
     */
    protected readonly ?array $topicPartitions;

    /**
     * @param array<string, list<int>|ElectLeadersRequestTopicPartitions>|null $topicPartitions Partitions to elect
     *        a leader for, as topic => partition ids, or {@see self::ALL_PARTITIONS}
     * @param int    $timeoutMs     How long the controller waits for the elections, in milliseconds
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        ?array $topicPartitions = self::ALL_PARTITIONS,
        /**
         * Milliseconds the controller waits for the elections to finish before it answers
         */
        protected readonly int $timeoutMs = self::DEFAULT_TIMEOUT_MS,
        string $clientId = '',
        int $correlationId = 0
    ) {
        if ($topicPartitions === null) {
            $this->topicPartitions = null;
        } else {
            $packed = [];
            foreach ($topicPartitions as $topic => $partitions) {
                $packed[$topic] = $partitions instanceof ElectLeadersRequestTopicPartitions
                    ? $partitions
                    : new ElectLeadersRequestTopicPartitions((string) $topic, $partitions);
            }
            $this->topicPartitions = $packed;
        }

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topicPartitions' => [
                'topic'                     => ElectLeadersRequestTopicPartitions::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
            'timeoutMs'       => BinarySchema::TYPE_INT32,
        ];
    }

    /**
     * Returns the partitions this request asks an election for, `null` for every partition of the cluster
     *
     * @return array<string, ElectLeadersRequestTopicPartitions>|null
     */
    public function getTopicPartitions(): ?array
    {
        return $this->topicPartitions;
    }

    /**
     * Returns the timeout of the election in milliseconds
     */
    public function getTimeoutMs(): int
    {
        return $this->timeoutMs;
    }
}
