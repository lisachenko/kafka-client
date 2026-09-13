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
 * Operations of the Kafka authorizer, and the bitfield that KIP-430 reports them in
 *
 * `AclOperation` @ 2.8.2 is the enumeration of everything an ACL may allow, and the codes below are its byte
 * values. **KIP-430** (Kafka 2.3) put them on the wire in a second shape: an `int32` **bitfield** in which the bit
 * with the number of an operation is set when the principal of the connection is authorized for it - the
 * `authorized_operations` of a Metadata v8 answer (per topic and for the cluster) and of a DescribeGroups v3
 * answer. `Utils.to32BitField` @ 2.8.2 builds it, `Utils.from32BitField` reads it back, and this class is both
 * halves for this client.
 *
 * The value {@see self::NOT_REQUESTED} (`-2147483648`, `Integer.MIN_VALUE`) is what a broker writes when the
 * request did not ask for the field, and it is **not** an empty set: a client that reads it has been told
 * nothing, while the bitfield `0` means "this principal may do nothing at all".
 *
 * A broker **without an authorizer** - the container of this line has none - answers every asked-for bitfield
 * with the full set of the operations of the resource, because `AclAuthorizer` is not there to refuse anything.
 *
 * @see docs/protocol/2.8.md, section "The authorized operations (v8, KIP-430)"
 */
final class AclOperation
{
    /**
     * An operation this client does not know; a broker never sends it
     */
    public const int UNKNOWN = 0;

    /**
     * Matches any operation, a filter value that never appears in an answer
     */
    public const int ANY = 1;

    public const int ALL = 2;

    public const int READ = 3;

    public const int WRITE = 4;

    public const int CREATE = 5;

    public const int DELETE = 6;

    public const int ALTER = 7;

    public const int DESCRIBE = 8;

    public const int CLUSTER_ACTION = 9;

    public const int DESCRIBE_CONFIGS = 10;

    public const int ALTER_CONFIGS = 11;

    public const int IDEMPOTENT_WRITE = 12;

    /**
     * Value of an `authorized_operations` field the request did not ask for: `Integer.MIN_VALUE`
     */
    public const int NOT_REQUESTED = -2147483648;

    /**
     * Names of the operations, indexed by their code, as `AclOperation` @ 2.8.2 spells them
     *
     * @var array<int, string>
     */
    public const array NAMES = [
        self::UNKNOWN          => 'UNKNOWN',
        self::ANY              => 'ANY',
        self::ALL              => 'ALL',
        self::READ             => 'READ',
        self::WRITE            => 'WRITE',
        self::CREATE           => 'CREATE',
        self::DELETE           => 'DELETE',
        self::ALTER            => 'ALTER',
        self::DESCRIBE         => 'DESCRIBE',
        self::CLUSTER_ACTION   => 'CLUSTER_ACTION',
        self::DESCRIBE_CONFIGS => 'DESCRIBE_CONFIGS',
        self::ALTER_CONFIGS    => 'ALTER_CONFIGS',
        self::IDEMPOTENT_WRITE => 'IDEMPOTENT_WRITE',
    ];

    /**
     * Tells whether a bitfield of KIP-430 names a given operation
     *
     * @param int $bitField  The `authorized_operations` of an answer
     * @param int $operation One of the constants of this class
     */
    public static function isAuthorized(int $bitField, int $operation): bool
    {
        if ($bitField === self::NOT_REQUESTED) {
            return false;
        }

        return ($bitField & (1 << $operation)) !== 0;
    }

    /**
     * Unpacks a bitfield of KIP-430 into the list of the operation codes it names, in ascending order
     *
     * `Utils.from32BitField` @ 2.8.2 does the same walk. A bitfield that was not asked for
     * ({@see self::NOT_REQUESTED}) unpacks to an empty list, which is why {@see self::wasRequested()} exists: the
     * two states are told apart by the raw value, not by the set.
     *
     * @return list<int>
     */
    public static function fromBitField(int $bitField): array
    {
        if ($bitField === self::NOT_REQUESTED) {
            return [];
        }

        $operations = [];
        foreach (array_keys(self::NAMES) as $operation) {
            if (($bitField & (1 << $operation)) !== 0) {
                $operations[] = $operation;
            }
        }

        return $operations;
    }

    /**
     * Packs a list of operation codes into the bitfield of KIP-430, the way `Utils.to32BitField` does
     *
     * @param list<int> $operations
     */
    public static function toBitField(array $operations): int
    {
        $bitField = 0;
        foreach ($operations as $operation) {
            $bitField |= 1 << $operation;
        }

        return $bitField;
    }

    /**
     * Tells whether a bitfield holds an answer at all, i.e. whether the request asked for it
     */
    public static function wasRequested(int $bitField): bool
    {
        return $bitField !== self::NOT_REQUESTED;
    }

    /**
     * Returns the names of the operations a bitfield names, for a message or a log line
     *
     * @return list<string>
     */
    public static function describe(int $bitField): array
    {
        return array_map(static fn(int $operation): string => self::NAMES[$operation], self::fromBitField($bitField));
    }
}
