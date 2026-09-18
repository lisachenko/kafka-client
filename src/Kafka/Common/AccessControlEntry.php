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
 * The permission half of an acl: who, from where, may do what, and whether it is allowed or denied
 *
 * <pre>
 *   AccessControlEntry => principal host operation permission_type
 *     principal       => STRING
 *     host            => STRING
 *     operation       => INT8
 *     permission_type => INT8
 * </pre>
 *
 * `org.apache.kafka.common.acl.AccessControlEntry` @ 3.9.2. The principal is the `<type>:<name>` **string** the
 * authorizer stores - `User:acltest`, the form {@see KafkaPrincipal::__toString()} produces - and not the two
 * fields the delegation-token apis carry a principal as; the host is an ip address or the literal
 * {@see self::ANY_HOST}, which is what an acl that names no host is stored with.
 *
 * Inside the `acls` array of a DescribeAcls answer this is a real structure of the specification and ends in a
 * tagged-field section of its own; inside a created acl and inside a matching acl of a DeleteAcls answer its four
 * fields are fields of the structure around it, where an {@see \Protocol\Kafka\Protocol\InlineStruct} embeds it.
 *
 * @see docs/protocol/3.9.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
class AccessControlEntry implements BinarySchemaInterface
{
    /**
     * The host of an acl that applies to every address, `AclEntry.WildcardHost`
     */
    public const string ANY_HOST = '*';

    /**
     * Principal the acl is written for, as the `<type>:<name>` string the authorizer stores
     */
    public string $principal;

    /**
     * Host the acl applies to, {@see self::ANY_HOST} for every address
     */
    public string $host;

    /**
     * Operation the acl covers, one of the constants of {@see AclOperation}
     */
    public int $operation;

    /**
     * {@see AclPermissionType::ALLOW} or {@see AclPermissionType::DENY}
     */
    public int $permissionType;

    public function __construct(
        KafkaPrincipal|string $principal = '',
        string $host = self::ANY_HOST,
        int $operation = AclOperation::ANY,
        int $permissionType = AclPermissionType::ANY
    ) {
        $this->principal      = (string) $principal;
        $this->host           = $host;
        $this->operation      = $operation;
        $this->permissionType = $permissionType;
    }

    /**
     * Builds an entry that allows a principal an operation, from every host unless one is named
     */
    public static function allow(
        KafkaPrincipal|string $principal,
        int $operation,
        string $host = self::ANY_HOST
    ): static {
        return new static($principal, $host, $operation, AclPermissionType::ALLOW);
    }

    /**
     * Builds an entry that denies a principal an operation, from every host unless one is named
     */
    public static function deny(
        KafkaPrincipal|string $principal,
        int $operation,
        string $host = self::ANY_HOST
    ): static {
        return new static($principal, $host, $operation, AclPermissionType::DENY);
    }

    /**
     * Returns the entry as the `principal host OPERATION PERMISSION` line of a message or a log
     */
    public function __toString(): string
    {
        return $this->principal
            . ' ' . $this->host
            . ' ' . AclOperation::nameOf($this->operation)
            . ' ' . AclPermissionType::nameOf($this->permissionType);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'principal'      => BinarySchema::TYPE_STRING,
            'host'           => BinarySchema::TYPE_STRING,
            'operation'      => BinarySchema::TYPE_INT8,
            'permissionType' => BinarySchema::TYPE_INT8,
        ];
    }
}
