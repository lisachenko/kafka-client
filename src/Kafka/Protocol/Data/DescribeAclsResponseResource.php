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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Common\AccessControlEntry;
use Protocol\Kafka\Common\AclBinding;
use Protocol\Kafka\Common\ResourcePattern;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\InlineStruct;

/**
 * One resource of a DescribeAcls answer, with every acl the filter matched on it
 *
 * <pre>
 *   DescribeAclsResponseResource => resource_type resource_name pattern_type [acls]
 *     resource_type => INT8
 *     resource_name => STRING
 *     pattern_type  => INT8                     -- since version 1 (KIP-290)
 *     acls          => principal host operation permission_type
 * </pre>
 *
 * `DescribeAclsResource` of `DescribeAclsResponse.json` @ 3.3.2. The answer is grouped by **pattern**, not by acl:
 * the three fields of the {@see ResourcePattern} come once and the acls that were written for it follow, so two
 * acls of the same topic are one resource with two entries and the same acl on a literal and on a prefixed
 * pattern is two resources with one entry each.
 *
 * {@see self::bindings()} flattens the group back into the {@see AclBinding} objects that
 * {@see \Protocol\Kafka\Admin\AdminClient::describeAcls()} answers with.
 *
 * @see docs/protocol/4.3.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
class DescribeAclsResponseResource implements BinarySchemaInterface
{
    /**
     * Resource the acls below were written for
     */
    public ResourcePattern $pattern;

    /**
     * Acls of that resource which the filter of the request matched
     *
     * @var list<AccessControlEntry>
     */
    public array $acls = [];

    /**
     * Returns one {@see AclBinding} per acl of this resource, i.e. the group flattened again
     *
     * @return list<AclBinding>
     */
    public function bindings(): array
    {
        $bindings = [];
        foreach ($this->acls as $entry) {
            $bindings[] = new AclBinding($this->pattern, $entry);
        }

        return $bindings;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            // The three pattern fields are fields of the resource entry, not a structure of the specification
            'pattern' => new InlineStruct(ResourcePattern::class),
            // An entry of this array IS a structure of the specification and ends in its own tag buffer in v2+
            'acls'    => [AccessControlEntry::class],
        ];
    }
}
