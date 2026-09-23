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
 * The resource half of an acl: a type, a name and how the name is matched
 *
 * <pre>
 *   ResourcePattern => resource_type resource_name pattern_type
 *     resource_type => INT8
 *     resource_name => STRING
 *     pattern_type  => INT8                    -- since version 1 (KIP-290)
 * </pre>
 *
 * `org.apache.kafka.common.resource.ResourcePattern` @ 3.9.2. The three fields are **not** a structure of the
 * specification: they are three ordinary fields of a created acl, of a described resource and of a matching acl of
 * a DeleteAcls answer, which is why every scheme embeds this class with an
 * {@see \Protocol\Kafka\Protocol\InlineStruct} and it never carries a tagged-field section of its own.
 *
 * A pattern is stored either as a {@see PatternType::LITERAL} one - the name is the resource, or the wildcard
 * {@see PatternType::WILDCARD_NAME} that stands for every resource of the type - or as a
 * {@see PatternType::PREFIXED} one, whose name is the prefix the covered resources start with.
 *
 * @see docs/protocol/4.3.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
class ResourcePattern implements BinarySchemaInterface
{
    /**
     * Type of the resource, one of the constants of {@see ResourceType}
     */
    public int $resourceType;

    /**
     * Name of the resource, or the prefix of the names it covers
     */
    public string $resourceName;

    /**
     * How the name is matched, {@see PatternType::LITERAL} or {@see PatternType::PREFIXED} in a stored acl
     */
    public int $patternType;

    public function __construct(
        int $resourceType = ResourceType::ANY,
        string $resourceName = '',
        int $patternType = PatternType::LITERAL
    ) {
        $this->resourceType = $resourceType;
        $this->resourceName = $resourceName;
        $this->patternType  = $patternType;
    }

    /**
     * Builds the literal pattern of one named resource
     */
    public static function literal(int $resourceType, string $resourceName): static
    {
        return new static($resourceType, $resourceName, PatternType::LITERAL);
    }

    /**
     * Builds the prefixed pattern of every resource of a type whose name starts with the given text
     */
    public static function prefixed(int $resourceType, string $prefix): static
    {
        return new static($resourceType, $prefix, PatternType::PREFIXED);
    }

    /**
     * Builds the pattern of the one cluster resource, which is always literal and always named `kafka-cluster`
     */
    public static function cluster(): static
    {
        return new static(ResourceType::CLUSTER, ResourceType::CLUSTER_NAME, PatternType::LITERAL);
    }

    /**
     * Returns the pattern in the `TYPE:PATTERN_TYPE:name` form of a log line or a message
     */
    public function __toString(): string
    {
        return ResourceType::nameOf($this->resourceType)
            . ':' . PatternType::nameOf($this->patternType)
            . ':' . $this->resourceName;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'resourceType' => BinarySchema::TYPE_INT8,
            'resourceName' => BinarySchema::TYPE_STRING,
            'patternType'  => BinarySchema::TYPE_INT8,
        ];
    }
}
