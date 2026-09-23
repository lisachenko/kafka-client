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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\Node;

/**
 * What {@see AdminClient::describeCluster()} knows about the cluster
 *
 * `DescribeClusterResult` of the Java admin client, flattened into one value object. Until Kafka 2.8 every one of
 * these fields had to be read out of a **Metadata** answer, which is a request about topics; KIP-700 gave them a
 * request of their own.
 *
 * Since Kafka 3.7 (KIP-919) the same request describes either half of a KRaft cluster, so the description says
 * which one it is: {@see self::$endpointType} is the {@see EndpointType} the server answered with, and the nodes
 * are its brokers or its controllers accordingly.
 *
 * @see docs/protocol/4.3.md, sections "DescribeCluster API (key 60, v0 and v1)" and "The endpoint type of KIP-919
 *      (v1)"
 */
final class ClusterDescription
{
    /**
     * The value of {@see self::$authorizedOperations} when the request did not ask for it (`Integer.MIN_VALUE`)
     */
    public const int OPERATIONS_NOT_REQUESTED = -2147483648;

    /**
     * @param string           $clusterId            Identifier of the cluster
     * @param int              $controllerId         Identifier of the active controller, -1 when there is none
     * @param array<int, Node> $nodes                Nodes of the described endpoint type, by node id
     * @param int              $authorizedOperations Acl bit field of KIP-430, or {@see self::OPERATIONS_NOT_REQUESTED}
     * @param EndpointType     $endpointType         Which half of the cluster {@see self::$nodes} holds (KIP-919,
     *        version 1); an answer below that version always describes the brokers
     */
    public function __construct(
        public readonly string $clusterId,
        public readonly int $controllerId,
        public readonly array $nodes,
        public readonly int $authorizedOperations = self::OPERATIONS_NOT_REQUESTED,
        public readonly EndpointType $endpointType = EndpointType::Broker
    ) {}

    /**
     * Returns the active controller of the cluster, or null while it has none
     */
    public function controller(): ?Node
    {
        return $this->nodes[$this->controllerId] ?? null;
    }

    /**
     * Returns whether this description is the one of the controllers of a KRaft cluster (KIP-919)
     */
    public function describesControllers(): bool
    {
        return $this->endpointType === EndpointType::Controller;
    }

    /**
     * Returns whether the answer carried the acl bit field at all
     */
    public function hasAuthorizedOperations(): bool
    {
        return $this->authorizedOperations !== self::OPERATIONS_NOT_REQUESTED;
    }
}
