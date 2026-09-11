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

namespace Protocol\Kafka\Common\Security;

use InvalidArgumentException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

use function strlen;

/**
 * A principal of the Kafka protocol: a type and a name
 *
 * <pre>
 *   KafkaPrincipal => principal_type name
 *     principal_type => STRING
 *     name           => STRING
 * </pre>
 *
 * `org.apache.kafka.common.security.auth.KafkaPrincipal` @ 1.1.1, which is the pair of strings the broker derives
 * from an authenticated channel: the SASL/PLAIN user `kafkatest` of the container is the principal
 * `User:kafkatest`, the type being {@see self::USER_TYPE} for every principal the shipped authorizer builds. A
 * custom `KafkaPrincipalBuilder` may use other types - which is why the type travels on the wire at all - but the
 * delegation token apis of Kafka 1.1 (KIP-48) refuse anything but `User` in the renewers of a token with the error
 * code 67 (`InvalidPrincipalType`).
 *
 * The class is the schema of the two-field struct that the four token apis embed: the `owner` of a
 * CreateDelegationToken answer, every entry of the `renewers` array of a request and of a described token, and
 * every entry of the `owners` array of a DescribeDelegationToken request. It is the same use that
 * {@see \Protocol\Kafka\Common\Node} has inside a Metadata answer.
 *
 * An unauthenticated channel has the principal `User:ANONYMOUS` ({@see self::anonymous()}), which is what the
 * PLAINTEXT listener of the container reports - and the reason why the token apis refuse it with the code 64.
 *
 * @see docs/protocol/2.8.md, section "CreateDelegationToken API (key 38, v0)"
 */
class KafkaPrincipal implements BinarySchemaInterface
{
    /**
     * Type of every principal the shipped `SimpleAclAuthorizer` builds, `KafkaPrincipal.USER_TYPE`
     */
    public const string USER_TYPE = 'User';

    /**
     * Name of the principal of a channel that did not authenticate, `KafkaPrincipal.ANONYMOUS`
     */
    public const string ANONYMOUS_NAME = 'ANONYMOUS';

    /**
     * Separator between the type and the name of the string form of a principal
     */
    public const string SEPARATOR = ':';

    /**
     * Type of the principal, `User` for everything the shipped authorizer builds
     */
    public string $principalType;

    /**
     * Name of the principal, i.e. the authenticated user
     */
    public string $name;

    public function __construct(string $principalType = self::USER_TYPE, string $name = '')
    {
        $this->principalType = $principalType;
        $this->name          = $name;
    }

    /**
     * Builds the principal of an ordinary user, i.e. one of the type `User`
     */
    public static function user(string $name): static
    {
        return new static(self::USER_TYPE, $name);
    }

    /**
     * Builds the principal of a channel that did not authenticate, `KafkaPrincipal.ANONYMOUS`
     */
    public static function anonymous(): static
    {
        return new static(self::USER_TYPE, self::ANONYMOUS_NAME);
    }

    /**
     * Parses the `<type>:<name>` form that every Kafka tool prints and accepts
     *
     * `SecurityUtils.parseKafkaPrincipal` @ 1.1.1 splits on the FIRST colon and refuses anything else, so the name
     * of a principal may itself contain colons but its type may not.
     *
     * @throws InvalidArgumentException If the string is not a `<type>:<name>` pair
     */
    public static function fromString(string $principal): static
    {
        $separator = strpos($principal, self::SEPARATOR);
        if ($separator === false || $separator === 0 || $separator === strlen($principal) - 1) {
            throw new InvalidArgumentException(
                "Expected a principal of the form <principalType>:<principalName>, got {$principal}"
            );
        }

        return new static(substr($principal, 0, $separator), substr($principal, $separator + 1));
    }

    /**
     * Normalizes a list of principals that are given as objects, as `<type>:<name>` strings, or as a mix of both
     *
     * Every api that takes principals - the renewers of a token, the owners of a describe request - accepts both
     * notations, so that a caller never has to build an object for the `User:kafkatest` a Kafka tool prints.
     *
     * @param iterable<self|string> $principals
     *
     * @return list<self>
     */
    public static function listOf(iterable $principals): array
    {
        $result = [];
        foreach ($principals as $principal) {
            $result[] = $principal instanceof self ? $principal : self::fromString($principal);
        }

        return $result;
    }

    /**
     * Returns the `<type>:<name>` form of the principal, `KafkaPrincipal.toString()`
     */
    public function __toString(): string
    {
        return $this->principalType . self::SEPARATOR . $this->name;
    }

    /**
     * Compares two principals by their type and their name, `KafkaPrincipal.equals()`
     */
    public function equals(self $other): bool
    {
        return $this->principalType === $other->principalType && $this->name === $other->name;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'principalType' => BinarySchema::TYPE_STRING,
            'name'          => BinarySchema::TYPE_STRING,
        ];
    }
}
