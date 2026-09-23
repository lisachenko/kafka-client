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

use Protocol\Kafka\Admin\EndpointType;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * DescribeCluster, version 1: the cluster without a topic in sight (ApiKey 60, Kafka 2.8, KIP-700)
 *
 * <pre>
 *   DescribeCluster Request (Version: 0 to 1) => include_cluster_authorized_operations endpoint_type
 *     include_cluster_authorized_operations => BOOLEAN
 *     endpoint_type                         => INT8     -- since version 1, 1 = brokers, 2 = controllers
 * </pre>
 *
 * Until Kafka 2.8 the only way to learn the cluster id, the controller and the list of brokers was a **Metadata**
 * request, which is a request about *topics* that happens to carry them; asking it for the cluster alone means
 * sending an empty topic array and paying for the topic machinery on the broker. KIP-700 gave the three fields a
 * request of their own - the smallest request of this protocol, a single boolean - and it is served by any broker.
 *
 * The flag asks for the `cluster_authorized_operations` bit field of KIP-430. Without it the answer carries
 * `Integer.MIN_VALUE` (`-2147483648`), which is the specification's own default and means "not asked".
 *
 * **Version 1 (KIP-919, Kafka 3.7) added the `endpoint_type`**: "Version 1 adds EndpointType for KIP-919 support"
 * is the comment above its `validVersions` in `DescribeClusterRequest.json` @ 3.7.2. A cluster without ZooKeeper
 * has two sets of nodes, and this byte says which of them the answer describes -
 * {@see \Protocol\Kafka\Admin\EndpointType::Broker} (1, the default and what every version 0 frame means) or
 * {@see \Protocol\Kafka\Admin\EndpointType::Controller} (2). A server that is not of the type that was asked for
 * answers **114** (`MismatchedEndpointType`) and a byte the enum has no case for is **115**
 * (`UnsupportedEndpointType`); {@see DescribeClusterRequestV0} is the frame below that, which cannot ask at all.
 *
 * @see docs/protocol/4.3.md, sections "DescribeCluster API (key 60, v0 and v1)" and "The endpoint type of KIP-919
 *      (v1)"
 */
class DescribeClusterRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_CLUSTER;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Which set of nodes this request asks about, as the byte of the wire
     *
     * @since Version 1 of protocol (Kafka 3.7, KIP-919)
     */
    protected readonly int $endpointType;

    /**
     * @param bool             $includeClusterAuthorizedOperations Whether to ask for the acl bit field of KIP-430
     * @param string           $clientId                           A user specified identifier for the client
     * @param int              $correlationId                      A value the broker passes back unmodified
     * @param EndpointType|int $endpointType                       Which set of nodes to describe (KIP-919, version
     *        1); a plain integer is the raw byte of the field, with which a caller can ask for a type that no
     *        version of the api defines and read the 115 it is refused with
     */
    public function __construct(
        protected readonly bool $includeClusterAuthorizedOperations = false,
        string $clientId = '',
        int $correlationId = 0,
        EndpointType|int $endpointType = EndpointType::Broker
    ) {
        $this->endpointType = $endpointType instanceof EndpointType ? $endpointType->value : $endpointType;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'includeClusterAuthorizedOperations' => BinarySchema::TYPE_BOOLEAN,
        ];
        if (static::VERSION >= 1) {
            $body['endpointType'] = BinarySchema::TYPE_INT8;
        }

        return $header + $body;
    }

    /**
     * Returns whether this request asks for the authorized operations of the cluster
     */
    public function includesClusterAuthorizedOperations(): bool
    {
        return $this->includeClusterAuthorizedOperations;
    }

    /**
     * Returns the raw `endpoint_type` byte of this request (KIP-919, version 1)
     */
    public function getEndpointTypeId(): int
    {
        return $this->endpointType;
    }

    /**
     * Returns the set of nodes this request asks about, {@see EndpointType::Unknown} for a byte the api does not
     * define (KIP-919, version 1)
     */
    public function getEndpointType(): EndpointType
    {
        return EndpointType::fromId($this->endpointType);
    }
}
