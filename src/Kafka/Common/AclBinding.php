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
 * One acl: a resource pattern and the permission that is written for it
 *
 * <pre>
 *   AclBinding => resource_type resource_name pattern_type principal host operation permission_type
 * </pre>
 *
 * `org.apache.kafka.common.acl.AclBinding` @ 3.9.2, the pair of a {@see ResourcePattern} and an
 * {@see AccessControlEntry}. It is the unit of {@see \Protocol\Kafka\Admin\AdminClient::createAcls()}, the unit a
 * describe answers and the unit a delete reports as deleted - and it is the wire structure of a `creations` entry
 * of a CreateAcls request as well, where the seven fields are flat: the two halves are embedded with an
 * {@see InlineStruct} and the entry of the array carries the one tagged-field section of the flexible version.
 *
 * @see docs/protocol/4.3.md, section "CreateAcls API (key 30, v0 to v3)"
 */
class AclBinding implements BinarySchemaInterface
{
    /**
     * Resource the acl is written for
     */
    public ResourcePattern $pattern;

    /**
     * Permission the acl grants or denies on that resource
     */
    public AccessControlEntry $entry;

    public function __construct(?ResourcePattern $pattern = null, ?AccessControlEntry $entry = null)
    {
        $this->pattern = $pattern ?? new ResourcePattern();
        $this->entry   = $entry ?? new AccessControlEntry();
    }

    /**
     * Builds an acl that allows a principal an operation on one literally named resource
     *
     * <code>
     *   AclBinding::allow(ResourceType::TOPIC, 'events', 'User:alice', AclOperation::READ);
     * </code>
     */
    public static function allow(
        int $resourceType,
        string $resourceName,
        KafkaPrincipal|string $principal,
        int $operation,
        int $patternType = PatternType::LITERAL,
        string $host = AccessControlEntry::ANY_HOST
    ): static {
        return new static(
            new ResourcePattern($resourceType, $resourceName, $patternType),
            AccessControlEntry::allow($principal, $operation, $host)
        );
    }

    /**
     * Builds an acl that denies a principal an operation on one literally named resource
     */
    public static function deny(
        int $resourceType,
        string $resourceName,
        KafkaPrincipal|string $principal,
        int $operation,
        int $patternType = PatternType::LITERAL,
        string $host = AccessControlEntry::ANY_HOST
    ): static {
        return new static(
            new ResourcePattern($resourceType, $resourceName, $patternType),
            AccessControlEntry::deny($principal, $operation, $host)
        );
    }

    /**
     * Returns the acl as the `TYPE:PATTERN_TYPE:name principal host OPERATION PERMISSION` line of a message
     */
    public function __toString(): string
    {
        return $this->pattern . ' ' . $this->entry;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            // Neither half is a structure of the specification: `CreateAclsRequest.json` @ 3.3.2 declares the
            // seven fields of an `AclCreation` flat, so both of them are inlined and carry no tag buffer
            'pattern' => new InlineStruct(ResourcePattern::class),
            'entry'   => new InlineStruct(AccessControlEntry::class),
        ];
    }
}
