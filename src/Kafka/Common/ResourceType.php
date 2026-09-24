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

namespace Protocol\Kafka\Common;

/**
 * Types of resource an ACL can be written for, `org.apache.kafka.common.resource.ResourceType` @ 3.9.2
 *
 * The `int8` that the three ACL apis carry as the `resource_type` of a created acl and of a described resource,
 * and as the `resource_type_filter` of a describe or delete filter. {@see self::ANY} is a **filter** value alone:
 * it matches every type and never appears in a stored acl, and {@see self::UNKNOWN} is what a client that is older
 * than the broker reads a type it does not know as - `ResourceType.fromCode` maps every unknown byte to it.
 *
 * **Kafka 3.3 added {@see self::USER}** with the version 3 of the three apis ("Version 3 adds user resource type"
 * of `DescribeAclsRequest.json` @ 3.3.2): the resource of KIP-373, on which the two operations
 * {@see AclOperation::CREATE_TOKENS} and {@see AclOperation::DESCRIBE_TOKENS} say who may issue a delegation
 * token for another principal and who may see it.
 *
 * The codes are **not** the ones of {@see \Protocol\Kafka\Admin\ConfigResource}, which names the resource of a
 * config api: `TOPIC` is 2 in both, but `GROUP` is 3 here and 8 there, and the broker resource has no ACL type at
 * all - a broker-wide permission is the {@see self::CLUSTER} resource.
 *
 * @see docs/protocol/4.3.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
final class ResourceType
{
    /**
     * A type this client does not know; a broker never sends it, and a request that carries it is refused
     */
    public const int UNKNOWN = 0;

    /**
     * Matches any resource type, a filter value that never appears in a stored acl
     */
    public const int ANY = 1;

    public const int TOPIC = 2;

    public const int GROUP = 3;

    public const int CLUSTER = 4;

    public const int TRANSACTIONAL_ID = 5;

    public const int DELEGATION_TOKEN = 6;

    /**
     * The `User` resource of KIP-373, the one type the version 3 of Kafka 3.3 added
     */
    public const int USER = 7;

    /**
     * Name of the one cluster resource of a Kafka cluster, `Resource.CLUSTER_NAME`
     *
     * The `CLUSTER` type has exactly one resource and the authorizer stores it under this name, which is what a
     * describe of every acl of the cluster answers and what `kafka-acls.sh --cluster` writes.
     */
    public const string CLUSTER_NAME = 'kafka-cluster';

    /**
     * Names of the types, indexed by their code, as `ResourceType` @ 3.9.2 spells them
     *
     * @var array<int, string>
     */
    public const array NAMES = [
        self::UNKNOWN          => 'UNKNOWN',
        self::ANY              => 'ANY',
        self::TOPIC            => 'TOPIC',
        self::GROUP            => 'GROUP',
        self::CLUSTER          => 'CLUSTER',
        self::TRANSACTIONAL_ID => 'TRANSACTIONAL_ID',
        self::DELEGATION_TOKEN => 'DELEGATION_TOKEN',
        self::USER             => 'USER',
    ];

    /**
     * Returns the name of a resource type, or the code itself when the broker used one this client does not know
     */
    public static function nameOf(int $resourceType): string
    {
        return self::NAMES[$resourceType] ?? (string) $resourceType;
    }
}
