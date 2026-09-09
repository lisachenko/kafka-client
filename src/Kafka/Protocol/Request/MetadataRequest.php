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
 *   TopicMetadataRequest => [TopicName]
 *     TopicName => string
 * </pre>
 *
 * In version 0 of this API the topic array is not nullable: an EMPTY array asks for every topic of the cluster.
 * The nullable array of the later protocol lines arrived with version 1 (Kafka 0.10.0).
 *
 * @see docs/protocol/0.9.0.md, section "Metadata API (key 3, v0)"
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
    public const int VERSION = 0;

    /**
     * @param list<string> $topics        Topics to fetch the metadata for, empty asks for every topic
     * @param string       $clientId      A user specified identifier for the client making the request
     * @param int          $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        protected array $topics = [],
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

        return $header + [
            'topics' => [BinarySchema::TYPE_STRING],
        ];
    }

    /**
     * Returns the list of topics this request asks the metadata for, empty means "every topic"
     *
     * @return list<string>
     */
    public function getTopics(): array
    {
        return $this->topics;
    }
}
