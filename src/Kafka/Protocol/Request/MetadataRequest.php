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
 * @date 14.07.2014
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;

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
 */
class MetadataRequest extends AbstractRequest
{
    public function __construct(/**
     * An array of topics to fetch metadata for. If no topics are specified fetch metadata for all topics.
     */
        protected array $topics = [],
        $apiVersion = 0,
        $correlationId = 0,
        $clientId = ''
    ) {
        parent::__construct(ApiKeys::METADATA, $apiVersion, $correlationId, $clientId);
    }

    /**
     * @return array
     */
    public function getTopics(): array
    {
        return explode(' ', $this->topics);
    }

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload = parent::packPayload();

        $totalTopics = count($this->topics);
        $payload .= pack('N', $totalTopics);
        foreach ($this->topics as $topic) {
            $length = strlen($topic);
            $payload .= pack("na{$length}", $length, $topic);
        }

        return $payload;
    }
}
