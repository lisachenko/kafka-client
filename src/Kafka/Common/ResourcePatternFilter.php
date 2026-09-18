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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The resource half of a describe or delete filter: which patterns it selects
 *
 * <pre>
 *   ResourcePatternFilter => resource_type_filter resource_name_filter pattern_type_filter
 *     resource_type_filter => INT8
 *     resource_name_filter => NULLABLE_STRING
 *     pattern_type_filter  => INT8                    -- since version 1 (KIP-290)
 * </pre>
 *
 * `org.apache.kafka.common.resource.ResourcePatternFilter` @ 3.9.2, the filter counterpart of
 * {@see ResourcePattern}: the same three fields, with {@see ResourceType::ANY} matching every type, a **null**
 * name matching every name and the two filter-only pattern types {@see PatternType::ANY} and
 * {@see PatternType::MATCH} next to the literal and the prefixed one.
 *
 * The three fields are fields of the request around them, not a structure of the specification, so they are
 * embedded with an {@see \Protocol\Kafka\Protocol\InlineStruct} and carry no tagged-field section.
 *
 * @see docs/protocol/3.9.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
class ResourcePatternFilter implements BinarySchemaInterface
{
    /**
     * Type of resource to match, {@see ResourceType::ANY} for every type
     */
    public int $resourceType;

    /**
     * Name to match, or null for every name
     */
    public ?string $resourceName;

    /**
     * Pattern type to match, {@see PatternType::ANY} or {@see PatternType::MATCH} for more than one
     */
    public int $patternType;

    public function __construct(
        int $resourceType = ResourceType::ANY,
        ?string $resourceName = null,
        int $patternType = PatternType::ANY
    ) {
        $this->resourceType = $resourceType;
        $this->resourceName = $resourceName;
        $this->patternType  = $patternType;
    }

    /**
     * The filter that matches every resource pattern of the cluster, `ResourcePatternFilter.ANY`
     */
    public static function any(): static
    {
        return new static(ResourceType::ANY, null, PatternType::ANY);
    }

    /**
     * Matches every pattern of one resource type, whatever its name and its pattern type
     */
    public static function ofType(int $resourceType): static
    {
        return new static($resourceType, null, PatternType::ANY);
    }

    /**
     * Matches the patterns that **apply to** one resource: its literal name, every prefix of it and the wildcard
     */
    public static function matching(int $resourceType, string $resourceName): static
    {
        return new static($resourceType, $resourceName, PatternType::MATCH);
    }

    /**
     * Matches one stored pattern exactly, i.e. the one this filter is the copy of
     */
    public static function of(ResourcePattern $pattern): static
    {
        return new static($pattern->resourceType, $pattern->resourceName, $pattern->patternType);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'resourceType' => BinarySchema::TYPE_INT8,
            'resourceName' => BinarySchema::TYPE_NULLABLE_STRING,
            'patternType'  => BinarySchema::TYPE_INT8,
        ];
    }
}
