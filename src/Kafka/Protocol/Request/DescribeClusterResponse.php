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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeClusterBroker;
use Protocol\Kafka\Protocol\Data\DescribeClusterBrokerV1;

/**
 * DescribeCluster response object, version 2 (key 60, Kafka 2.8, KIP-700)
 *
 * <pre>
 *   DescribeCluster Response (Version: 0 to 2) => throttle_time_ms error_code error_message endpoint_type
 *                                                 cluster_id controller_id [brokers]
 *                                                 cluster_authorized_operations
 *     throttle_time_ms              => INT32
 *     error_code                    => INT16
 *     error_message                 => COMPACT_NULLABLE_STRING
 *     endpoint_type                 => INT8    -- since version 1, 1 = brokers, 2 = controllers
 *     cluster_id                    => COMPACT_STRING
 *     controller_id                 => INT32   (-1 while the cluster has no controller)
 *     brokers                       => broker_id host port rack is_fenced  -- is_fenced since version 2
 *     cluster_authorized_operations => INT32
 * </pre>
 *
 * **`cluster_authorized_operations` is `Integer.MIN_VALUE` when the request did not ask for it**, which is the
 * default of the specification and not an error; a request that does ask gets the bit field of KIP-430, one bit
 * per `AclOperation` the caller may perform on the cluster. On a broker that runs **without** an authorizer every
 * operation is allowed, so what comes back is the whole set `AclEntry.supportedOperations(CLUSTER)` names.
 *
 * **Version 1 (KIP-919, Kafka 3.7) echoes the `endpoint_type`** the server really described - the field sits
 * between the error message and the cluster id - and "makes MISMATCHED_ENDPOINT_TYPE and UNSUPPORTED_ENDPOINT_TYPE
 * valid top-level response error codes" (`DescribeClusterResponse.json` @ 3.7.2). Both refusals are answered
 * **without** a cluster id, a broker list or a type: `AuthHelper.computeDescribeClusterResponse` @ 3.9.2 returns a
 * bare `DescribeClusterResponseData` with the code and the message, so the `endpoint_type` of such an answer is the
 * `"default": "1"` of the schema and says nothing. {@see DescribeClusterResponseV0} is the answer one version
 * lower, which has no type at all.
 *
 * **Version 2 (KIP-1073, Kafka 4.0) appends `is_fenced` to every broker** ({@see DescribeClusterBroker}) and lists
 * the fenced brokers when the request asked for them with `include_fenced_brokers`; nothing else of the answer
 * changed. {@see DescribeClusterResponseV1} is the answer below it, whose brokers are
 * {@see DescribeClusterBrokerV1} entries.
 *
 * @see docs/protocol/4.3.md, sections "DescribeCluster API (key 60, v0 to v2)", "The endpoint type of KIP-919
 *      (v1)" and "The fenced brokers of KIP-1073 (v2)"
 */
class DescribeClusterResponse extends AbstractResponse
{
    /**
     * The value of `cluster_authorized_operations` when the request did not ask for it (`Integer.MIN_VALUE`)
     */
    public const int OPERATIONS_NOT_REQUESTED = -2147483648;

    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the request, 0 when the cluster could be described
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Which set of nodes the answer describes, as the byte of the wire
     *
     * @since Version 1 of protocol (Kafka 3.7, KIP-919)
     */
    public int $endpointType = EndpointType::Broker->value;

    /**
     * Identifier of the cluster the answering broker belongs to
     */
    public string $clusterId;

    /**
     * Identifier of the active controller, -1 while the cluster has none
     */
    public int $controllerId = -1;

    /**
     * Brokers of the cluster, indexed by their id
     *
     * @var array<int, DescribeClusterBroker>
     */
    public array $brokers = [];

    /**
     * Bit field of the acl operations the caller may perform on the cluster, or
     * {@see self::OPERATIONS_NOT_REQUESTED} when the request did not ask for it
     */
    public int $clusterAuthorizedOperations = self::OPERATIONS_NOT_REQUESTED;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
        if (static::VERSION >= 1) {
            $body['endpointType'] = BinarySchema::TYPE_INT8;
        }

        return $header + $body + [
            'clusterId'                   => BinarySchema::TYPE_STRING,
            'controllerId'                => BinarySchema::TYPE_INT32,
            'brokers'                     => [
                'brokerId' => static::VERSION >= 2 ? DescribeClusterBroker::class : DescribeClusterBrokerV1::class,
            ],
            'clusterAuthorizedOperations' => BinarySchema::TYPE_INT32,
        ];
    }

    /**
     * Returns the set of nodes this answer describes, {@see EndpointType::Unknown} for a byte the api does not
     * define (KIP-919, version 1)
     */
    public function getEndpointType(): EndpointType
    {
        return EndpointType::fromId($this->endpointType);
    }
}
