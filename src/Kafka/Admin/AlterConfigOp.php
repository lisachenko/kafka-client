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

namespace Protocol\Kafka\Admin;

use InvalidArgumentException;

/**
 * One change an IncrementalAlterConfigs request asks for: an option, a value and what to do with them
 *
 * `org.apache.kafka.clients.admin.AlterConfigOp` @ 2.8.2, the argument of KIP-339. Where {@see AlterConfigsRequest}
 * takes the WHOLE configuration a resource should have afterwards and resets everything a caller forgot, this one
 * names a single option and an operation on it, and leaves every option it does not mention alone.
 *
 * <code>
 *   $admin->incrementalAlterConfigs([
 *       ConfigResource::topic('events')->key() => [
 *           AlterConfigOp::set('retention.ms', '3600000'),
 *           AlterConfigOp::delete('segment.bytes'),
 *           AlterConfigOp::append('cleanup.policy', 'compact'),
 *       ],
 *   ]);
 * </code>
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0 and v1)"
 */
final class AlterConfigOp
{
    /**
     * Set the option to the given value, `OpType.SET` @ 2.8.2
     */
    public const int SET = 0;

    /**
     * Reset the option to its default, `OpType.DELETE` @ 2.8.2; the value travels as a null string
     */
    public const int DELETE = 1;

    /**
     * Add the given values to a **list** option, `OpType.APPEND` @ 2.8.2
     *
     * Only an option whose `ConfigDef.Type` is `LIST` accepts it - `cleanup.policy` of a topic, for instance.
     * Anything else is refused by the broker, see the section of the document.
     */
    public const int APPEND = 2;

    /**
     * Remove the given values from a **list** option, `OpType.SUBTRACT` @ 2.8.2
     *
     * The same restriction as {@see self::APPEND}. Removing a value the list does not have is legal and changes
     * nothing, and removing every value leaves the EMPTY list instead of restoring the default.
     */
    public const int SUBTRACT = 3;

    /**
     * Name of every operation, for an exception or a log line
     *
     * @var array<int, string>
     */
    private const array NAMES = [
        self::SET      => 'SET',
        self::DELETE   => 'DELETE',
        self::APPEND   => 'APPEND',
        self::SUBTRACT => 'SUBTRACT',
    ];

    /**
     * @param string      $name      Name of the option, e.g. `retention.ms`
     * @param string|null $value     Value of the option as text, `null` only for {@see self::DELETE}
     * @param int         $operation What to do with it, one of the constants of this class
     */
    public function __construct(
        public readonly string $name,
        public readonly ?string $value = null,
        public readonly int $operation = self::SET
    ) {
        if (!isset(self::NAMES[$operation])) {
            throw new InvalidArgumentException("Unknown config operation {$operation}");
        }
    }

    /**
     * Sets the option to the given value
     */
    public static function set(string $name, string $value): self
    {
        return new self($name, $value, self::SET);
    }

    /**
     * Resets the option to its default
     *
     * The value of a DELETE travels as the **null** string, and it is the only operation whose value may be null:
     * `ZkAdminManager.incrementalAlterConfigs` @ 2.8.2 refuses every other one with `Null value not supported for :
     * <op>:<name>` and the error code 42.
     */
    public static function delete(string $name): self
    {
        return new self($name, null, self::DELETE);
    }

    /**
     * Adds the given values - a comma separated list - to a list option
     */
    public static function append(string $name, string $value): self
    {
        return new self($name, $value, self::APPEND);
    }

    /**
     * Removes the given values - a comma separated list - from a list option
     */
    public static function subtract(string $name, string $value): self
    {
        return new self($name, $value, self::SUBTRACT);
    }

    /**
     * Returns whether the given value is one of the operations of the protocol
     */
    public static function isKnown(int $operation): bool
    {
        return isset(self::NAMES[$operation]);
    }

    /**
     * Returns the name of an operation, for an exception or a log line
     */
    public static function nameOf(int $operation): string
    {
        return self::NAMES[$operation] ?? "UNKNOWN({$operation})";
    }
}
