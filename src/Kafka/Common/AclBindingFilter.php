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

use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\InlineStruct;

/**
 * Which acls a describe or a delete selects: a resource-pattern filter and an entry filter
 *
 * <pre>
 *   AclBindingFilter => resource_type_filter resource_name_filter pattern_type_filter
 *                       principal_filter host_filter operation permission_type
 * </pre>
 *
 * `org.apache.kafka.common.acl.AclBindingFilter` @ 3.9.2, the filter counterpart of {@see AclBinding}: the seven
 * fields of a DescribeAcls request body and of one entry of the `filters` array of a DeleteAcls request. Every
 * field of it may be a wildcard - {@see self::any()} is the filter that selects **every** acl of the cluster, and
 * a delete with it removes them all.
 *
 * `matchesAtMostOne()` is what the Java client calls a filter that can select no more than a single acl, i.e. one
 * whose seven fields are all concrete; anything else may match many, which is why a DeleteAcls answer reports
 * every matching acl per filter instead of a count.
 *
 * @see docs/protocol/3.9.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
class AclBindingFilter implements BinarySchemaInterface
{
    /**
     * Which resource patterns the filter selects
     */
    public ResourcePatternFilter $patternFilter;

    /**
     * Which acls of those patterns the filter selects
     */
    public AccessControlEntryFilter $entryFilter;

    public function __construct(
        ?ResourcePatternFilter $patternFilter = null,
        ?AccessControlEntryFilter $entryFilter = null
    ) {
        $this->patternFilter = $patternFilter ?? ResourcePatternFilter::any();
        $this->entryFilter   = $entryFilter ?? AccessControlEntryFilter::any();
    }

    /**
     * The filter that matches every acl of the cluster, `AclBindingFilter.ANY`
     */
    public static function any(): static
    {
        return new static(ResourcePatternFilter::any(), AccessControlEntryFilter::any());
    }

    /**
     * Matches every acl of one resource type
     */
    public static function ofResourceType(int $resourceType): static
    {
        return new static(ResourcePatternFilter::ofType($resourceType), AccessControlEntryFilter::any());
    }

    /**
     * Matches every acl written for one principal, on any resource
     */
    public static function ofPrincipal(KafkaPrincipal|string $principal): static
    {
        return new static(ResourcePatternFilter::any(), AccessControlEntryFilter::ofPrincipal($principal));
    }

    /**
     * Matches every acl that **applies to** one resource: the literal pattern, every prefix and the wildcard
     */
    public static function matching(int $resourceType, string $resourceName): static
    {
        return new static(
            ResourcePatternFilter::matching($resourceType, $resourceName),
            AccessControlEntryFilter::any()
        );
    }

    /**
     * Matches exactly the acl given, which is what a delete of one known binding sends
     */
    public static function of(AclBinding $binding): static
    {
        return new static(
            ResourcePatternFilter::of($binding->pattern),
            AccessControlEntryFilter::of($binding->entry)
        );
    }

    /**
     * Tells whether this filter can match no more than one acl, `AclBindingFilter.matchesAtMostOne()`
     */
    public function matchesAtMostOne(): bool
    {
        return $this->patternFilter->resourceType !== ResourceType::ANY
            && $this->patternFilter->resourceName !== null
            && PatternType::isSpecific($this->patternFilter->patternType)
            && $this->entryFilter->principal !== null
            && $this->entryFilter->host !== null
            && $this->entryFilter->operation !== AclOperation::ANY
            && $this->entryFilter->permissionType !== AclPermissionType::ANY;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            // The seven fields are flat in both apis - the body of a DescribeAcls request and one entry of the
            // `filters` array of a DeleteAcls request - so neither half is a structure with a tag buffer
            'patternFilter' => new InlineStruct(ResourcePatternFilter::class),
            'entryFilter'   => new InlineStruct(AccessControlEntryFilter::class),
        ];
    }
}
