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

use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\RestorableTrait;
use Protocol\Kafka\Common\TopicMetadata;

/**
 * Metadata response object
 *
 * <pre>
 *   MetadataResponse => [Broker][TopicMetadata]
 * </pre>
 *
 * Version 0 has neither a `ClusterId` (added in 0.10.1) nor a `ControllerId` (added in version 1 of this API).
 *
 * A broker that has just booted answers with an EMPTY broker array while its metadata cache has not been filled by
 * the controller yet - that is "not ready, retry", never "the cluster has no brokers".
 *
 * @see docs/protocol/0.8.2.md, sections "Metadata API (key 3, v0)" and "Cluster readiness"
 */
class MetadataResponse extends AbstractResponse
{
    use RestorableTrait;

    /**
     * List of broker metadata info, indexed by the node id
     *
     * @var array<int, Node>
     */
    public array $brokers = [];

    /**
     * List of topics, indexed by the topic name
     *
     * @var array<string, TopicMetadata>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            // Both arrays are indexed by the field the cluster looks an entry up by: Cluster::nodeById() resolves a
            // partition leader by its broker id and Cluster::partitionsForTopic() a topic by its name
            'brokers' => ['nodeId' => Node::class],
            'topics'  => ['topic' => TopicMetadata::class],
        ];
    }
}
