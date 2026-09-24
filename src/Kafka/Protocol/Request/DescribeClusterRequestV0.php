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

/**
 * DescribeCluster request of version 0 (Kafka 2.8, KIP-700): the one boolean, without the endpoint type
 *
 * The smallest request of this protocol, and the shape it had before KIP-919 (Kafka 3.7) appended the
 * `endpoint_type` byte to it: a frame of this version cannot name a set of nodes at all, so it always describes
 * the **brokers**, which is the `"default": "1"` the specification gives the field for exactly that reason. It is
 * also the version below the two error codes of the KIP - `AuthHelper.computeDescribeClusterResponse` @ 3.9.2
 * answers a version 0 request the **42** (`InvalidRequest`) where a version 1 request is answered 114 or 115 -
 * although no frame of this version can reach that branch, because it carries no type to mismatch.
 *
 * @see docs/protocol/4.3.md, sections "DescribeCluster API (key 60, v0 to v2)" and "The endpoint type of KIP-919
 *      (v1)"
 */
final class DescribeClusterRequestV0 extends DescribeClusterRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
