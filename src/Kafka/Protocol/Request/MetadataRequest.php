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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * This API answers the following questions:
 *
 *      What topics exist?
 *      How many partitions does each topic have?
 *      Which broker is currently the leader for each partition?
 *      What is the host and port for each of these brokers?
 *
 * This is the only request that can be addressed to any broker in the cluster.
 * Since there may be many topics the client can give an optional list of topic names in order to only return metadata
 * for a subset of topics.
 *
 * The metadata returned is at the partition level, but grouped together by topic for convenience and to avoid
 * redundancy. For each partition the metadata contains the information for the leader as well as for all the replicas
 * and the list of replicas that are currently in-sync.
 *
 * Note: If "auto.create.topics.enable" is set in the broker configuration, a topic metadata request will create the
 * topic with the default replication factor and number of partitions.
 *
 * <pre>
 *   Metadata Request (Version: 1 and 2) => [topics]
 *     topics => NULLABLE_ARRAY of STRING
 * </pre>
 *
 * `METADATA_REQUEST_V2 = METADATA_REQUEST_V1` in `Protocol.java` @ 0.10.2.2 - the two versions send the very same
 * frame, only the answer of version 2 carries the `ClusterId` on top. Version 1 (Kafka 0.10.0) made the topic array
 * NULLABLE, which is the whole point of it: a client can now tell the two intentions apart that version 0
 * ({@see MetadataRequestV0}) had to express with the same empty array.
 *
 * | topics | frame         | 0.10.2.2 broker answers                                                     |
 * |--------|---------------|-----------------------------------------------------------------------------|
 * | `null` | `ff ff ff ff` | every topic of the cluster, the internal `__consumer_offsets` included       |
 * | `[]`   | `00 00 00 00` | no topic at all - the brokers of the cluster and an empty topic array        |
 *
 * An empty array creates nothing either: `KafkaApis.handleTopicMetadataRequest` @ 0.10.2.2 only auto-creates the
 * topics that the request NAMES, so `[]` is the cheapest way to ask a broker for the members of the cluster.
 *
 * @see docs/protocol/0.10.2.md, section "Metadata API (key 3, v0, v1 and v2)"
 */
class MetadataRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::METADATA;

    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @param list<string>|null $topics        Topics to fetch the metadata for, null asks for every topic
     * @param string            $clientId      A user specified identifier for the client making the request
     * @param int               $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        protected ?array $topics = null,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $topics = static::VERSION >= 1
            ? [BinarySchema::TYPE_STRING, BinarySchema::FLAG_NULLABLE => true]
            : [BinarySchema::TYPE_STRING];

        return $header + [
            'topics' => $topics,
        ];
    }

    /**
     * Returns the list of topics this request asks the metadata for, null means "every topic"
     *
     * @return list<string>|null
     */
    public function getTopics(): ?array
    {
        return $this->topics;
    }
}
