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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The permission half of a describe or delete filter: which acls of a pattern it selects
 *
 * <pre>
 *   AccessControlEntryFilter => principal_filter host_filter operation permission_type
 *     principal_filter => NULLABLE_STRING
 *     host_filter      => NULLABLE_STRING
 *     operation        => INT8
 *     permission_type  => INT8
 * </pre>
 *
 * `org.apache.kafka.common.acl.AccessControlEntryFilter` @ 3.9.2: a **null** principal matches every principal, a
 * null host every host, {@see AclOperation::ANY} every operation and {@see AclPermissionType::ANY} both an allow
 * and a deny acl. Unlike the pattern half, the two names are matched **literally** - there is no prefix logic
 * here, and a principal filter either is the whole `User:name` string of the acl or matches nothing.
 *
 * @see docs/protocol/4.3.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
class AccessControlEntryFilter implements BinarySchemaInterface
{
    /**
     * Principal to match, or null for every principal
     */
    public ?string $principal;

    /**
     * Host to match, or null for every host
     */
    public ?string $host;

    /**
     * Operation to match, {@see AclOperation::ANY} for every operation
     */
    public int $operation;

    /**
     * Permission type to match, {@see AclPermissionType::ANY} for both
     */
    public int $permissionType;

    public function __construct(
        KafkaPrincipal|string|null $principal = null,
        ?string $host = null,
        int $operation = AclOperation::ANY,
        int $permissionType = AclPermissionType::ANY
    ) {
        $this->principal      = $principal === null ? null : (string) $principal;
        $this->host           = $host;
        $this->operation      = $operation;
        $this->permissionType = $permissionType;
    }

    /**
     * The filter that matches every acl, `AccessControlEntryFilter.ANY`
     */
    public static function any(): static
    {
        return new static(null, null, AclOperation::ANY, AclPermissionType::ANY);
    }

    /**
     * Matches every acl of one principal, whatever it allows or denies
     */
    public static function ofPrincipal(KafkaPrincipal|string $principal): static
    {
        return new static($principal, null, AclOperation::ANY, AclPermissionType::ANY);
    }

    /**
     * Matches one stored entry exactly, i.e. the one this filter is the copy of
     */
    public static function of(AccessControlEntry $entry): static
    {
        return new static($entry->principal, $entry->host, $entry->operation, $entry->permissionType);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'principal'      => BinarySchema::TYPE_NULLABLE_STRING,
            'host'           => BinarySchema::TYPE_NULLABLE_STRING,
            'operation'      => BinarySchema::TYPE_INT8,
            'permissionType' => BinarySchema::TYPE_INT8,
        ];
    }
}
