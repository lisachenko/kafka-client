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

use Protocol\Kafka\Common\AclBindingFilter;
use Protocol\Kafka\Protocol\ApiKeys;

/**
 * DeleteAcls, version 3: removes the acls that a filter matches (ApiKey 31, Kafka 0.11, KIP-140)
 *
 * <pre>
 *   DeleteAcls Request (Version: 3) => [filters]
 *     filters => resource_type_filter resource_name_filter pattern_type_filter
 *                principal_filter host_filter operation permission_type
 * </pre>
 *
 * A delete never names an acl, it names a **filter** - the same {@see AclBindingFilter} a DescribeAcls request
 * carries, one or more of them in one frame - and the broker removes every acl that matches. That is why the
 * answer repeats each matched acl in full: it is the only way to learn what was removed.
 *
 * {@see AclBindingFilter::of()} builds the filter of one known acl, which is the delete of exactly that acl and
 * nothing else; {@see AclBindingFilter::any()} removes **every acl of the cluster**.
 *
 * **The whole request is authorized before a filter is read**: `AclApis.handleDeleteAcls` @ 3.9.2 asks the
 * authorizer for `ALTER` on the `CLUSTER` resource, and a principal that may not do it is answered **31** in
 * every filter result. Like a creation, a deletion of a KRaft node is a controller write.
 *
 * **The versions.** Version 1 (Kafka 2.0) added the pattern type of KIP-290, version 2 (Kafka 2.4) is the first
 * **flexible** one and **version 3 (Kafka 3.3) adds the user resource type** of KIP-373 ("Version 3 adds the user
 * resource type" of `DeleteAclsRequest.json` @ 3.3.2). No field changed with it.
 *
 * @see docs/protocol/4.3.md, section "DeleteAcls API (key 31, v0 to v3)"
 */
class DeleteAclsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DELETE_ACLS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * The version 2 of Kafka 2.4 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Filters to apply, in the order the answer reports them in
     *
     * @var list<AclBindingFilter>
     */
    protected readonly array $filters;

    /**
     * @param list<AclBindingFilter> $filters       Filters of the acls to remove
     * @param string                 $clientId      A user specified identifier for the client making the request
     * @param int                    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(array $filters = [], string $clientId = '', int $correlationId = 0)
    {
        $this->filters = array_values($filters);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the filters the request applies
     *
     * @return list<AclBindingFilter>
     */
    public function getFilters(): array
    {
        return $this->filters;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'filters' => [AclBindingFilter::class],
        ];
    }
}
