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

namespace Protocol\Kafka\Protocol;

/**
 * Declares a nested object that the **specification does not have**: its fields are fields of the structure around
 * it, and it exists only to give a PHP caller something better than two loose strings.
 *
 * The distinction does not matter in the plain encoding - a structure is not counted or delimited on the wire, so a
 * group of fields and a nested structure are the same bytes - but it matters in a **flexible** one: every real
 * structure of a flexible version ends in a tagged-field section, and a field group must not, or the frame carries a
 * `00` the broker does not expect.
 *
 * The one place of Kafka 2.8.2 where this repository has such a group is the **owner of a delegation token**:
 * `CreateDelegationTokenResponse.json` declares `PrincipalType` and `PrincipalName` as two ordinary fields of the
 * answer, while this package reads them into a {@see \Protocol\Kafka\Common\Security\KafkaPrincipal}. The very same
 * class is a *real* structure in the request of the same api, where the renewers are a `[]CreatableRenewers` - so
 * the marker belongs to the **field**, not to the class:
 *
 * <code>
 * return $header + [
 *     'errorCode'      => BinarySchema::TYPE_INT16,
 *     'owner'          => new InlineStruct(KafkaPrincipal::class),   // two fields of the answer
 *     'issueTimestamp' => BinarySchema::TYPE_INT64,
 *     // …
 * ];
 * </code>
 *
 * A wave-2 ticket needs it whenever it wraps two or more flat fields of a specification in an object of its own;
 * an entry of a `[]Something` array of the specification is never one.
 *
 * @see docs/protocol/2.8.md, section "Implementation model"
 */
final class InlineStruct
{
    /**
     * @param class-string<BinarySchemaInterface> $type Class whose scheme is inlined into the enclosing structure
     */
    public function __construct(public readonly string $type) {}
}
