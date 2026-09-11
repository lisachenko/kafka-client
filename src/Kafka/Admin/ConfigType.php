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

/**
 * The data type of a configuration option, as a DescribeConfigs **v3** answer reports it (Kafka 2.6, KIP-569)
 *
 * The ids are the ordinals of the wire enum `DescribeConfigsResponse.ConfigType` @ 2.8.2, which is the
 * `ConfigEntry.ConfigType` of the Java admin client with the same members in the same order, and the broker fills
 * the field from the `ConfigDef.Type` of the option (`ConfigHelper.configResponseType` @ 2.8.2). A constants class
 * rather than an enum, for the reason of {@see ConfigSource}: the value travels as an int8 and a later release may
 * send an id this client does not know, which {@see self::fromWire()} folds into {@see self::UNKNOWN}.
 *
 * The type is what tells a caller how to read - and how to write back - the text of
 * {@see ConfigEntry::$value}: a `LIST` is comma separated and is the only kind of option an
 * {@see AlterConfigOp::APPEND} or an {@see AlterConfigOp::SUBTRACT} may touch, and a `PASSWORD` is the type of
 * every option whose value the broker replaces with `null` on the wire.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v4)"
 */
final class ConfigType
{
    /**
     * The broker does not know the option, or its `ConfigDef.Type` is none of the ones below
     *
     * `ConfigHelper.configResponseType` @ 2.8.2 answers this for an option that is not in the `ConfigDef` of the
     * resource at all, and it is also the `"default": "0"` of the field - which is what an entry of a **lower**
     * api version decodes to, because those frames carry no type byte.
     */
    public const int UNKNOWN = 0;

    /**
     * `ConfigDef.Type.BOOLEAN`, whose value is the text `true` or `false`
     */
    public const int BOOLEAN = 1;

    /**
     * `ConfigDef.Type.STRING`
     */
    public const int STRING = 2;

    /**
     * `ConfigDef.Type.INT`, a 32-bit integer as text
     */
    public const int INT = 3;

    /**
     * `ConfigDef.Type.SHORT`, a 16-bit integer as text
     */
    public const int SHORT = 4;

    /**
     * `ConfigDef.Type.LONG`, a 64-bit integer as text
     */
    public const int LONG = 5;

    /**
     * `ConfigDef.Type.DOUBLE`
     */
    public const int DOUBLE = 6;

    /**
     * `ConfigDef.Type.LIST`, a comma separated list - the only type APPEND and SUBTRACT accept
     */
    public const int LIST = 7;

    /**
     * `ConfigDef.Type.CLASS`, the fully qualified name of a Java class
     */
    public const int CLASS_NAME = 8;

    /**
     * `ConfigDef.Type.PASSWORD`, whose value the broker never sends - the entry carries `null` instead
     */
    public const int PASSWORD = 9;

    /**
     * Name of every type id, as `ConfigEntry.ConfigType` @ 2.8.2 spells it
     *
     * @var array<int, string>
     */
    private const array NAMES = [
        self::UNKNOWN    => 'UNKNOWN',
        self::BOOLEAN    => 'BOOLEAN',
        self::STRING     => 'STRING',
        self::INT        => 'INT',
        self::SHORT      => 'SHORT',
        self::LONG       => 'LONG',
        self::DOUBLE     => 'DOUBLE',
        self::LIST       => 'LIST',
        self::CLASS_NAME => 'CLASS',
        self::PASSWORD   => 'PASSWORD',
    ];

    /**
     * This class is a namespace of constants and is never instantiated
     */
    private function __construct() {}

    /**
     * Returns the type id that travelled on the wire, folding an id this client does not know into {@see UNKNOWN}
     */
    public static function fromWire(int $configType): int
    {
        return isset(self::NAMES[$configType]) ? $configType : self::UNKNOWN;
    }

    /**
     * Returns the name of a type id, for a message or a log line
     *
     * The name of {@see self::CLASS_NAME} is `CLASS`, the name of the Java enum member; the constant of this class
     * cannot be called that, because `class` is a reserved word of PHP.
     */
    public static function nameOf(int $configType): string
    {
        return self::NAMES[$configType] ?? self::NAMES[self::UNKNOWN];
    }
}
