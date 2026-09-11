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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * DescribeCluster, version 0: the cluster without a topic in sight (ApiKey 60, Kafka 2.8, KIP-700)
 *
 * <pre>
 *   DescribeCluster Request (Version: 0) => include_cluster_authorized_operations
 *     include_cluster_authorized_operations => BOOLEAN
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
 * @see docs/protocol/2.8.md, section "DescribeCluster API (key 60, v0)"
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
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * @param bool   $includeClusterAuthorizedOperations Whether to ask for the acl bit field of KIP-430
     * @param string $clientId                           A user specified identifier for the client
     * @param int    $correlationId                      A value the broker passes back unmodified
     */
    public function __construct(
        protected readonly bool $includeClusterAuthorizedOperations = false,
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
            'includeClusterAuthorizedOperations' => BinarySchema::TYPE_BOOLEAN,
        ];
    }

    /**
     * Returns whether this request asks for the authorized operations of the cluster
     */
    public function includesClusterAuthorizedOperations(): bool
    {
        return $this->includeClusterAuthorizedOperations;
    }
}
