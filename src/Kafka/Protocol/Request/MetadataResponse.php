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

use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\NodeV0;
use Protocol\Kafka\Common\RestorableTrait;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\TopicMetadataV0;
use Protocol\Kafka\Common\TopicMetadataV1;
use Protocol\Kafka\Common\TopicMetadataV5;
use Protocol\Kafka\Common\TopicMetadataV7;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Metadata response object, version 7 (key 3)
 *
 * <pre>
 *   Metadata Response (Version: 7) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
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
 *       partition_metadata => partition_error_code partition_id leader leader_epoch [replicas] [isr]
 *                           [offline_replicas]
 *         leader_epoch     => INT32           -- since version 7
 *         offline_replicas => ARRAY of INT32  -- since version 5
 * </pre>
 *
 * The six versions of this answer differ in what surrounds the topics, and each field arrived in a different
 * Kafka release: version 1 (Kafka 0.10.0) added `ControllerId`, the `Rack` of every broker and the `IsInternal`
 * flag of every topic; version 2 (Kafka 0.10.1) inserted `ClusterId` BEFORE the controller id, which is why a v2
 * answer can not be read with the v1 class and vice versa; version 3 (KIP-124, Kafka 0.11) opened the answer with
 * a `throttle_time_ms`; and version 4 changed nothing at all here - `METADATA_RESPONSE_V4 = METADATA_RESPONSE_V3`
 * in `MetadataResponse.schemaVersions()` @ 1.1.1 - because what it added, `allow_auto_topic_creation`, is a field
 * of the REQUEST ({@see MetadataRequest}).
 *
 * **Version 5 (Kafka 1.0, KIP-112/113) appended `offline_replicas` to every partition entry**, the replicas of the
 * partition that are not available because their broker is down or the log directory that holds them failed, see
 * {@see \Protocol\Kafka\Common\PartitionMetadata::$offlineReplicas}. {@see MetadataResponseV4},
 * {@see MetadataResponseV3}, {@see MetadataResponseV2}, {@see MetadataResponseV1} and {@see MetadataResponseV0}
 * lower the version constant this scheme follows. **Version 6 (Kafka 2.0, KIP-219) changed the frame no more than
 * version 4 did** - `MetadataResponse.json` @ 2.8.2 carries no field of it, its comment is "Starting in version 6,
 * on quota violation, brokers send out responses before throttling" - and {@see MetadataResponseV5} decodes the
 * same bytes; what version 6 states is that the client understands when a throttled answer arrives, and waits the
 * reported time out itself.
 *
 * **Version 7 (Kafka 2.1, KIP-320) inserted `leader_epoch` into every partition entry**, behind the leader id, see
 * {@see \Protocol\Kafka\Common\PartitionMetadata::$leaderEpoch}: the epoch the leader of that partition is
 * currently on. It is the half of KIP-320 that tells a consumer *that* a leader changed;
 * {@see MetadataResponseV6} keeps the answer that carries no epoch.
 *
 * `ControllerId` is the broker id of the active controller, or `-1` (`MetadataResponse.NO_CONTROLLER_ID` @
 * 1.1.1) while the cluster is electing one; it is what {@see \Protocol\Kafka\Admin\AdminClient::findController()}
 * asks for. `ClusterId` is the identifier that a 0.10.1 broker generates once and keeps in ZooKeeper under
 * `/cluster/id`, so every broker of one cluster answers the same one; it is null when the answer comes from a
 * broker that has none.
 *
 * A broker that has just booted answers with an EMPTY broker array while its metadata cache has not been filled by
 * the controller yet - that is "not ready, retry", never "the cluster has no brokers".
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v8)" and "Cluster readiness"
 */
class MetadataResponse extends AbstractResponse
{
    use RestorableTrait;

    /**
     * Version of the Metadata API that this class unpacks
     */
    public const int VERSION = 8;

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
     * Operations the principal of this connection is authorized for on the **cluster**, the bitfield of KIP-430.
     *
     * {@see \Protocol\Kafka\Common\AclOperation} reads it; {@see \Protocol\Kafka\Common\AclOperation::NOT_REQUESTED}
     * is what a broker writes when the request did not set `include_cluster_authorized_operations`, and what every
     * answer below version 8 leaves here. The field exists in the versions **8 to 10** only: KIP-700 moved the
     * cluster-wide operations to the DescribeCluster api and version 11 dropped it again, which is beyond this
     * line.
     *
     * @since Version 8 of protocol (Kafka 2.3, KIP-430)
     */
    public int $clusterAuthorizedOperations = AclOperation::NOT_REQUESTED;

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
        if (static::VERSION >= 8) {
            $body['clusterAuthorizedOperations'] = BinarySchema::TYPE_INT32;
        }

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
        return match (true) {
            static::VERSION >= 8 => TopicMetadata::class,
            static::VERSION >= 7 => TopicMetadataV7::class,
            static::VERSION >= 5 => TopicMetadataV5::class,
            static::VERSION >= 1 => TopicMetadataV1::class,
            default              => TopicMetadataV0::class,
        };
    }
}
