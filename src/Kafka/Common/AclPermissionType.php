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
 * Whether an acl allows or denies, `org.apache.kafka.common.acl.AclPermissionType` @ 3.9.2
 *
 * The `int8` `permission_type` of every acl of the three ACL apis. A stored acl is either {@see self::ALLOW} or
 * {@see self::DENY}, {@see self::ANY} matches both in a filter, and {@see self::UNKNOWN} is what a client reads a
 * value it does not know as.
 *
 * **A DENY wins over an ALLOW**, whatever the order they were written in: `StandardAuthorizer` @ 3.9.2 walks the
 * acls that match an operation and answers `DENIED` as soon as one of them denies it, which is why a deny acl
 * cannot be overruled by adding another allow one.
 *
 * @see docs/protocol/4.3.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
final class AclPermissionType
{
    /**
     * A permission type this client does not know; a broker never sends it
     */
    public const int UNKNOWN = 0;

    /**
     * Matches both an allow and a deny acl, a filter value that never appears in a stored acl
     */
    public const int ANY = 1;

    public const int DENY = 2;

    public const int ALLOW = 3;

    /**
     * Names of the permission types, indexed by their code, as `AclPermissionType` @ 3.9.2 spells them
     *
     * @var array<int, string>
     */
    public const array NAMES = [
        self::UNKNOWN => 'UNKNOWN',
        self::ANY     => 'ANY',
        self::DENY    => 'DENY',
        self::ALLOW   => 'ALLOW',
    ];

    /**
     * Returns the name of a permission type, or the code itself when the broker used one this client does not know
     */
    public static function nameOf(int $permissionType): string
    {
        return self::NAMES[$permissionType] ?? (string) $permissionType;
    }
}
