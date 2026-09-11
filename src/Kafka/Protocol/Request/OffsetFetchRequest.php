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

use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * OffsetFetch, version 7: the offsets that a consumer group committed, read from `__consumer_offsets`
 *
 * This API reads back the offsets that were committed for a consumer group with the OffsetCommit API, so it has to
 * be sent to the coordinator of that group.
 *
 * <pre>
 *   OffsetFetch Request (Version: 2 to 7) => group_id [topics] require_stable
 *     group_id       => STRING
 *     topics         => topic [partitions]     -- NULLABLE since version 2
 *       topic      => STRING
 *       partitions => partition
 *         partition => INT32
 *     require_stable => BOOLEAN            -- since version 7
 * </pre>
 *
 * Version 2 (KIP-88, Kafka 0.10.2) made the topic array **nullable**, and that is the only change of the request:
 * a `null` array - `ff ff ff ff` on the wire - asks the coordinator for every topic-partition the group has a
 * committed offset for, which is what an administrative tool needs and what {@see self::forAllTopics()} builds. An
 * **empty** array - `00 00 00 00` - is a different request that names no topic at all and is answered with an empty
 * response; the two must not be confused.
 *
 * Version 3 (KIP-124, Kafka 0.11) left the request untouched - `OFFSET_FETCH_REQUEST_V3 = OFFSET_FETCH_REQUEST_V2`
 * in `Protocol.java` @ 0.11.0.3 - and only added the leading `throttle_time_ms` to the answer, so
 * {@see OffsetFetchRequestV2} puts the same bytes on the wire and reads its answer with
 * {@see OffsetFetchResponseV2}.
 *
 * Version 4 (KIP-219, Kafka 2.0) changed neither half of the api: `OffsetFetchRequest.json` @ 2.8.2 introduces
 * nothing between the nullable topic array of version 2 and the `require_stable` of version 7, and the answer only
 * gains the `committed_leader_epoch` of version 5 (KIP-320, Kafka 2.1). Sending version 4 promises that this client
 * honours the `throttle_time_ms` of the answer itself, because a throttled 2.x broker answers **first** and mutes
 * the channel afterwards.
 *
 * **Version 5 (Kafka 2.1, KIP-320) changed the ANSWER alone**: every partition of it gained a
 * `committed_leader_epoch` behind the committed offset, the epoch an OffsetCommit v6 stored with it. The request
 * is byte for byte the one of version 2, so {@see OffsetFetchRequestV4} and {@see OffsetFetchRequestV3} send the
 * same body one and two api versions lower and only read their answers with the matching response class.
 *
 * **Version 7 (KIP-447, Kafka 2.5) appended the boolean `require_stable`** behind the topic array, the first
 * field the request gained since version 2. A `true` asks the coordinator to answer a partition whose last
 * offset commit belongs to a transaction that has **not been committed yet** with the retriable error code
 * **88** (`UnstableOffsetCommit`) instead of that offset, so that a consumer of a read-committed pipeline never
 * reads an offset the transaction may still roll back. `false` - the default, and the only behaviour of every
 * version below - answers the offset of the last commit whatever its transaction is doing.
 * {@see OffsetFetchRequestV6} is the same frame without the flag.
 *
 * Versions 0 and 1 have no nullable array ({@see OffsetFetchRequestV1}, {@see OffsetFetchRequestV0}) and are
 * identical to each other on the wire: they only differ in where the broker reads the offsets from - ZooKeeper for
 * version 0, the `__consumer_offsets` topic of the cluster for version 1 and above. Asking those versions for all
 * topics is refused here with an {@see UnsupportedVersionException}, exactly as `OffsetFetchRequest.Builder.build()`
 * @ 0.11.0.3 does; sending a `-1` topic array with version 1 makes the broker close the connection.
 *
 * @see docs/protocol/2.8.md, sections "OffsetFetch API (key 9, v0 to v7)" and "Stable offsets and the 88 of
 *      KIP-447 (Kafka 2.5)"
 */
class OffsetFetchRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::OFFSET_FETCH;

    /**
     * @inheritdoc
     */
    public const int VERSION = 7;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 6;

    /**
     * Partitions whose offsets are requested, indexed by the topic they belong to, or null for every topic
     *
     * @var array<string, PartitionsForTopic>|null
     */
    protected readonly ?array $topicPartitions;

    /**
     * @param string $consumerGroup   Name of the consumer group
     * @param array<string, list<int>|PartitionsForTopic>|null $topicPartitions Partitions to fetch, per topic, or
     *        null to ask for every topic-partition the group has committed an offset for (version 2 and above)
     * @param string $clientId        Unique client identifier
     * @param int    $correlationId   Correlated request id
     */
    public function __construct(
        protected readonly string $consumerGroup,
        ?array $topicPartitions,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Whether the coordinator has to hold back an offset whose transaction has not been committed yet.
         *
         * `false` - the default and every version below 7 - answers the offset of the last commit, committed or
         * not; `true` (KIP-447, Kafka 2.5) makes the coordinator answer the partition with the **retriable** error
         * code 88 (`UnstableOffsetCommit`) instead, until the transaction that wrote the pending offset ends.
         *
         * @since Version 7 of protocol
         */
        protected readonly bool $requireStable = false
    ) {
        if ($topicPartitions === null) {
            if (static::VERSION < 2) {
                throw new UnsupportedVersionException(
                    [
                        'error'   => sprintf(
                            'The version %d of the OffsetFetch api can not ask for every topic of a group, '
                            . 'the nullable topic array arrived with the version 2 in Kafka 0.10.2',
                            static::VERSION
                        ),
                        'groupId' => $consumerGroup,
                    ]
                );
            }
            $this->topicPartitions = null;
        } else {
            $packedTopicPartitions = [];
            foreach ($topicPartitions as $topic => $partitions) {
                $packedTopicPartitions[$topic] = $partitions instanceof PartitionsForTopic
                    ? $partitions
                    : new PartitionsForTopic((string) $topic, array_values($partitions));
            }
            $this->topicPartitions = $packedTopicPartitions;
        }

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds the request that asks for every topic-partition the group has a committed offset for (version 2)
     *
     * `OffsetFetchRequest.forAllPartitions()` @ 0.10.2.2 is the same shortcut.
     */
    public static function forAllTopics(
        string $consumerGroup,
        string $clientId = '',
        int $correlationId = 0,
        bool $requireStable = false
    ): static {
        return new static($consumerGroup, null, $clientId, $correlationId, $requireStable);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header          = parent::getScheme();
        $topicPartitions = ['topic' => PartitionsForTopic::class];
        if (static::VERSION >= 2) {
            $topicPartitions[BinarySchema::FLAG_NULLABLE] = true;
        }

        $body = [
            'consumerGroup'   => BinarySchema::TYPE_STRING,
            'topicPartitions' => $topicPartitions,
        ];
        if (static::VERSION >= 7) {
            $body['requireStable'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }
}
