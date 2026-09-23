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
 * How the name of a resource pattern is matched, `org.apache.kafka.common.resource.PatternType` @ 3.9.2
 *
 * KIP-290 (Kafka 2.0) turned the resource of an acl from a plain name into a **pattern**: the version 1 of the
 * three ACL apis added this `int8` next to the name, and since then an acl either names one resource exactly
 * ({@see self::LITERAL}) or every resource whose name starts with the given text ({@see self::PREFIXED}).
 *
 * Only those two are ever **stored**. The other three are values of a *filter*:
 *
 *  - {@see self::ANY} matches a pattern of any type whose name is the one given - the literal `events` and every
 *    prefix of it that covers `events`;
 *  - {@see self::MATCH} is the wider question "which acls apply to this resource": it matches the literal pattern
 *    of the name, every prefixed pattern the name starts with, and the wildcard `*`
 *    (`AclBindingFilter.matchesAtMostOne` @ 3.9.2 is false for it);
 *  - {@see self::UNKNOWN} is what a client reads a type it does not know as, and what a broker refuses.
 *
 * The wildcard `*` is not a pattern type but a literal pattern whose **name** is `*`
 * ({@see self::WILDCARD_NAME}), which is what `kafka-acls.sh --topic '*'` writes.
 *
 * @see docs/protocol/4.3.md, section "DescribeAcls API (key 29, v0 to v3)"
 */
final class PatternType
{
    /**
     * A pattern type this client does not know; a broker never sends it
     */
    public const int UNKNOWN = 0;

    /**
     * Matches a pattern of any type with the given name, a filter value alone
     */
    public const int ANY = 1;

    /**
     * Matches every pattern that applies to the given resource name, a filter value alone
     */
    public const int MATCH = 2;

    /**
     * The name of the pattern is the name of the resource, or the wildcard `*`
     */
    public const int LITERAL = 3;

    /**
     * The name of the pattern is a prefix of the names of the resources it covers
     */
    public const int PREFIXED = 4;

    /**
     * The literal name that stands for every resource of a type, `ResourcePattern.WILDCARD_RESOURCE`
     */
    public const string WILDCARD_NAME = '*';

    /**
     * Names of the pattern types, indexed by their code, as `PatternType` @ 3.9.2 spells them
     *
     * @var array<int, string>
     */
    public const array NAMES = [
        self::UNKNOWN  => 'UNKNOWN',
        self::ANY      => 'ANY',
        self::MATCH    => 'MATCH',
        self::LITERAL  => 'LITERAL',
        self::PREFIXED => 'PREFIXED',
    ];

    /**
     * Tells whether a pattern type can be stored in an acl, i.e. whether it is LITERAL or PREFIXED
     *
     * `PatternType.isSpecific()` @ 3.9.2. A creation that carries anything else is refused by the broker with the
     * error code 42 (`InvalidRequest`).
     */
    public static function isSpecific(int $patternType): bool
    {
        return $patternType === self::LITERAL || $patternType === self::PREFIXED;
    }

    /**
     * Returns the name of a pattern type, or the code itself when the broker used one this client does not know
     */
    public static function nameOf(int $patternType): string
    {
        return self::NAMES[$patternType] ?? (string) $patternType;
    }
}
