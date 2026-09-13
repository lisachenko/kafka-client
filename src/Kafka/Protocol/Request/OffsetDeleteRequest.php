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
use Protocol\Kafka\Protocol\Data\OffsetDeleteRequestTopic;

/**
 * OffsetDelete, version 0: throws committed offsets of a group away (ApiKey 47, Kafka 2.4, KIP-496)
 *
 * <pre>
 *   OffsetDelete Request (Version: 0) => group_id [topics]
 *     group_id => STRING
 *     topics   => name [partitions]
 *       name       => STRING
 *       partitions => partition_index
 *         partition_index => INT32
 * </pre>
 *
 * KIP-496 added the api for what `kafka-consumer-groups.sh --delete-offsets` had no protocol for before: a group
 * that keeps running can be made to forget the committed offsets of *some* of its partitions, where
 * {@see DeleteGroupsRequest} can only throw the whole group away. The coordinator writes a tombstone into
 * `__consumer_offsets` for every deleted partition, so an {@see OffsetFetchRequest} of the group answers `-1` for
 * it afterwards.
 *
 * **The request goes to the group coordinator** ({@see GroupCoordinatorRequest}); every other broker answers it
 * with the top-level error code 16 (NotCoordinatorForGroup).
 *
 * The api **is not flexible and stays at the version 0 through Kafka 2.8.2**: `OffsetDeleteRequest.json` @ 2.8.2
 * declares `"validVersions": "0"` and no `flexibleVersions`, so this is one of the two client apis of the 2.x line
 * that the compact encoding never reaches - the other one is SaslHandshake (17), and the only two further
 * specifications of 2.8.2 without a `flexibleVersions` are the KRaft-internal BeginQuorumEpoch (53) and
 * EndQuorumEpoch (54), which no client sends.
 *
 * The Java admin client calls the call `deleteConsumerGroupOffsets()`, which is the name
 * {@see \Protocol\Kafka\Admin\AdminClient::deleteConsumerGroupOffsets()} carries.
 *
 * @see docs/protocol/2.8.md, section "OffsetDelete API (key 47, v0)"
 */
class OffsetDeleteRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::OFFSET_DELETE;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Topics of this request, indexed by their name
     *
     * @var array<string, OffsetDeleteRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param string $groupId Name of the group whose committed offsets are deleted
     * @param array<string, iterable<int, int>|OffsetDeleteRequestTopic> $topics Partition indexes to delete the
     *        committed offset of, per topic; an empty array is a legal request that deletes nothing
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        protected readonly string $groupId,
        array $topics,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packed = [];
        foreach ($topics as $topic => $partitions) {
            $packed[$topic] = $partitions instanceof OffsetDeleteRequestTopic
                ? $partitions
                : new OffsetDeleteRequestTopic((string) $topic, $partitions);
        }
        $this->topics = $packed;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'groupId' => BinarySchema::TYPE_STRING,
            'topics'  => ['name' => OffsetDeleteRequestTopic::class],
        ];
    }

    /**
     * Returns the name of the group whose committed offsets this request deletes
     */
    public function getGroupId(): string
    {
        return $this->groupId;
    }

    /**
     * Returns the topics of this request, indexed by their name
     *
     * @return array<string, OffsetDeleteRequestTopic>
     */
    public function getTopics(): array
    {
        return $this->topics;
    }
}
