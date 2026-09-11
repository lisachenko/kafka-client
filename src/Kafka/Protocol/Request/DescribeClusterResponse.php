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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeClusterBroker;

/**
 * DescribeCluster response object, version 0 (key 60, Kafka 2.8, KIP-700)
 *
 * <pre>
 *   DescribeCluster Response (Version: 0) => throttle_time_ms error_code error_message cluster_id controller_id
 *                                            [brokers] cluster_authorized_operations
 *     throttle_time_ms              => INT32
 *     error_code                    => INT16
 *     error_message                 => COMPACT_NULLABLE_STRING
 *     cluster_id                    => COMPACT_STRING
 *     controller_id                 => INT32   (-1 while the cluster has no controller)
 *     brokers                       => broker_id host port rack
 *     cluster_authorized_operations => INT32
 * </pre>
 *
 * **`cluster_authorized_operations` is `Integer.MIN_VALUE` when the request did not ask for it**, which is the
 * default of the specification and not an error; a request that does ask gets the bit field of KIP-430, one bit
 * per `AclOperation` the caller may perform on the cluster. On a broker that runs **without** an authorizer every
 * operation is allowed, so what comes back is the whole set `AclEntry.supportedOperations(CLUSTER)` names.
 *
 * @see docs/protocol/2.8.md, section "DescribeCluster API (key 60, v0)"
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
    public const int VERSION = 0;

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

        return $header + [
            'throttleTimeMs'              => BinarySchema::TYPE_INT32,
            'errorCode'                   => BinarySchema::TYPE_INT16,
            'errorMessage'                => BinarySchema::TYPE_NULLABLE_STRING,
            'clusterId'                   => BinarySchema::TYPE_STRING,
            'controllerId'                => BinarySchema::TYPE_INT32,
            'brokers'                     => ['brokerId' => DescribeClusterBroker::class],
            'clusterAuthorizedOperations' => BinarySchema::TYPE_INT32,
        ];
    }
}
