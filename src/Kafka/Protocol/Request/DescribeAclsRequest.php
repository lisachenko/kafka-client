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

use Protocol\Kafka\Common\AccessControlEntryFilter;
use Protocol\Kafka\Common\AclBindingFilter;
use Protocol\Kafka\Common\ResourcePatternFilter;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\InlineStruct;

/**
 * DescribeAcls, version 3: reads the acls of the cluster through a filter (ApiKey 29, Kafka 0.11, KIP-140)
 *
 * <pre>
 *   DescribeAcls Request (Version: 3) => resource_type_filter resource_name_filter pattern_type_filter
 *                                        principal_filter host_filter operation permission_type
 *     resource_type_filter => INT8
 *     resource_name_filter => NULLABLE_STRING
 *     pattern_type_filter  => INT8                    -- since version 1 (KIP-290)
 *     principal_filter     => NULLABLE_STRING
 *     host_filter          => NULLABLE_STRING
 *     operation            => INT8
 *     permission_type      => INT8
 * </pre>
 *
 * The api of KIP-140 (Kafka 0.11) that reads what the **authorizer** of the cluster holds. It only does anything
 * on a broker that has one: without an `authorizer.class.name` every version of it is answered **54**
 * (`SecurityDisabled`) and the message `No Authorizer is configured on the broker`, which is what the containers
 * of every line below this one answered and why they never implemented the three ACL apis. The node of this line
 * runs `org.apache.kafka.metadata.authorizer.StandardAuthorizer`, so the answers here are real.
 *
 * **The request is one filter**, seven flat fields that this client models as an {@see AclBindingFilter}: which
 * resource patterns to look at, and which of their acls. Every field may be a wildcard - `ANY` for the three
 * codes, `null` for the three names - and {@see AclBindingFilter::any()} is the filter that matches every acl of
 * the cluster. The two pattern types {@see \Protocol\Kafka\Common\PatternType::ANY} and
 * {@see \Protocol\Kafka\Common\PatternType::MATCH} exist for this request alone: `MATCH` asks the wider question
 * "which acls apply to this resource" and answers the literal pattern, every prefixed pattern the name starts
 * with and the wildcard `*`.
 *
 * **The whole request is authorized before the filter is read.** `AclApis.handleDescribeAcls` @ 3.9.2 asks the
 * authorizer for `DESCRIBE` on the `CLUSTER` resource and answers a principal that may not do it with **31**
 * (`ClusterAuthorizationFailed`) and no resource at all - the filter is never applied, so a principal cannot read
 * even the acls that are written for itself.
 *
 * **The versions.** Version 1 (Kafka 2.0) added the `pattern_type_filter` of KIP-290, version 2 (Kafka 2.4) is
 * the first **flexible** one - the request header v2 and the compact strings - and **version 3 (Kafka 3.3) adds
 * the user resource type**, i.e. it says that the client understands {@see \Protocol\Kafka\Common\ResourceType::USER}
 * of KIP-373 in an answer ("Version 3 adds user resource type" of `DescribeAclsRequest.json` @ 3.3.2). Not one
 * field changed with it, which is why this client keeps no version below 3: the api is implemented on this line
 * for the first time and the node announces 0 to 3.
 *
 * @see docs/protocol/3.9.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
class DescribeAclsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_ACLS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * The version 2 of Kafka 2.4 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Which resource patterns the request asks about
     */
    protected readonly ResourcePatternFilter $patternFilter;

    /**
     * Which acls of those patterns the request asks about
     */
    protected readonly AccessControlEntryFilter $entryFilter;

    /**
     * @param AclBindingFilter|null $filter        Acls to describe, null for every acl of the cluster
     * @param string                $clientId      A user specified identifier for the client making the request
     * @param int                   $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(?AclBindingFilter $filter = null, string $clientId = '', int $correlationId = 0)
    {
        $filter ??= AclBindingFilter::any();

        $this->patternFilter = $filter->patternFilter;
        $this->entryFilter   = $filter->entryFilter;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the filter the request carries
     */
    public function getFilter(): AclBindingFilter
    {
        return new AclBindingFilter($this->patternFilter, $this->entryFilter);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            // The seven fields are flat fields of the body, so neither half carries a tag buffer of its own
            'patternFilter' => new InlineStruct(ResourcePatternFilter::class),
            'entryFilter'   => new InlineStruct(AccessControlEntryFilter::class),
        ];
    }
}
