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
use Protocol\Kafka\Common\NodeV0;
use Protocol\Kafka\Common\RestorableTrait;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\TopicMetadataV0;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Metadata response object, version 4 (key 3)
 *
 * <pre>
 *   Metadata Response (Version: 3 and 4) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
 *     throttle_time_ms => INT32     -- since version 3
 *     brokers => node_id host port rack
 *       node_id => INT32
 *       host    => STRING
 *       port    => INT32
 *       rack    => NULLABLE_STRING
 *     cluster_id    => NULLABLE_STRING
 *     controller_id => INT32
 *     topic_metadata => topic_error_code topic is_internal [partition_metadata]
 *       topic_error_code => INT16
 *       topic            => STRING
 *       is_internal      => BOOLEAN
 *       partition_metadata => partition_error_code partition_id leader [replicas] [isr]
 * </pre>
 *
 * The five versions of this answer differ in what surrounds the topics, and each field arrived in a different
 * Kafka release: version 1 (Kafka 0.10.0) added `ControllerId`, the `Rack` of every broker and the `IsInternal`
 * flag of every topic; version 2 (Kafka 0.10.1) inserted `ClusterId` BEFORE the controller id, which is why a v2
 * answer can not be read with the v1 class and vice versa; version 3 (KIP-124, Kafka 0.11) opened the answer with
 * a `throttle_time_ms`; and version 4 changed nothing at all here - `METADATA_RESPONSE_V4 = METADATA_RESPONSE_V3`
 * in `Protocol.java` @ 0.11.0.3 - because what it added, `allow_auto_topic_creation`, is a field of the REQUEST
 * ({@see MetadataRequest}). {@see MetadataResponseV3}, {@see MetadataResponseV2}, {@see MetadataResponseV1} and
 * {@see MetadataResponseV0} lower the version constant this scheme follows.
 *
 * `ControllerId` is the broker id of the active controller, or `-1` (`MetadataResponse.NO_CONTROLLER_ID` @
 * 0.11.0.3) while the cluster is electing one; it is what {@see \Protocol\Kafka\Admin\AdminClient::findController()}
 * asks for. `ClusterId` is the identifier that a 0.10.1 broker generates once and keeps in ZooKeeper under
 * `/cluster/id`, so every broker of one cluster answers the same one; it is null when the answer comes from a
 * broker that has none.
 *
 * A broker that has just booted answers with an EMPTY broker array while its metadata cache has not been filled by
 * the controller yet - that is "not ready, retry", never "the cluster has no brokers".
 *
 * @see docs/protocol/0.11.0.md, sections "Metadata API (key 3, v0 to v4)" and "Cluster readiness"
 */
class MetadataResponse extends AbstractResponse
{
    use RestorableTrait;

    /**
     * Version of the Metadata API that this class unpacks
     */
    public const int VERSION = 4;

    /**
     * Broker id that the answer reports while the cluster has no active controller
     */
    public const int NO_CONTROLLER_ID = -1;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 3 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * List of broker metadata info, indexed by the node id
     *
     * @var array<int, Node>
     */
    public array $brokers = [];

    /**
     * The cluster id that this broker belongs to, null when the broker does not have one.
     *
     * @since Version 2 of protocol
     */
    public ?string $clusterId = null;

    /**
     * The broker id of the controller broker, -1 while no broker is the controller.
     *
     * Null when the answer was a version 0 one, which does not carry the field at all.
     *
     * @since Version 1 of protocol
     */
    public ?int $controllerId = null;

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

        // Both arrays are indexed by the field the cluster looks an entry up by: Cluster::nodeById() resolves a
        // partition leader by its broker id and Cluster::partitionsForTopic() a topic by its name
        $body = [];
        if (static::VERSION >= 3) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['brokers'] = ['nodeId' => static::nodeClass()];
        if (static::VERSION >= 2) {
            $body['clusterId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        if (static::VERSION >= 1) {
            $body['controllerId'] = BinarySchema::TYPE_INT32;
        }
        $body['topics'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a broker entry for the version of the API that this class unpacks
     *
     * @return class-string<Node>
     */
    protected static function nodeClass(): string
    {
        return static::VERSION >= 1 ? Node::class : NodeV0::class;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class unpacks
     *
     * @return class-string<TopicMetadata>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 1 ? TopicMetadata::class : TopicMetadataV0::class;
    }
}
