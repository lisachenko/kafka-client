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
 * Where the value of a configuration option comes from, as a DescribeConfigs v1 answer reports it (KIP-226)
 *
 * The ids are the ones of the wire enum `DescribeConfigsResponse.ConfigSource` @ 1.1.1 - `UNKNOWN_CONFIG(0)`,
 * `TOPIC_CONFIG(1)`, `DYNAMIC_BROKER_CONFIG(2)`, `DYNAMIC_DEFAULT_BROKER_CONFIG(3)`, `STATIC_BROKER_CONFIG(4)`,
 * `DEFAULT_CONFIG(5)` - and the names are the ones of the Java admin client's `ConfigEntry.ConfigSource`, whose enum
 * is unnumbered and calls the first two `UNKNOWN` and `DYNAMIC_TOPIC_CONFIG`. A constants class rather than an enum,
 * like {@see ConfigResource}: the value travels as an int8 and a broker of a later release may send an id this
 * client does not know, which {@see self::fromWire()} folds into {@see self::UNKNOWN} exactly as `ConfigSource.forId`
 * does.
 *
 * The order of the ids is the order in which a value wins: a dynamic broker config overrides a dynamic default
 * broker config, which overrides the `server.properties`, which overrides the built-in default. That is also the
 * order of the synonyms of an entry, whose first element is the one that won.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0, v1 and v2)"
 */
final class ConfigSource
{
    /**
     * A source this client does not know, `UNKNOWN_CONFIG` @ 1.1.1; also what a version 0 answer implies for a
     * resource type other than a topic or a broker
     */
    public const int UNKNOWN = 0;

    /**
     * The option stands in the ZooKeeper node of the topic, `TOPIC_CONFIG` (the Java `DYNAMIC_TOPIC_CONFIG`)
     */
    public const int TOPIC_CONFIG = 1;

    /**
     * KIP-226 set the option on THIS broker at runtime, `DYNAMIC_BROKER_CONFIG`
     */
    public const int DYNAMIC_BROKER_CONFIG = 2;

    /**
     * KIP-226 set the option for EVERY broker of the cluster at runtime, `DYNAMIC_DEFAULT_BROKER_CONFIG`
     */
    public const int DYNAMIC_DEFAULT_BROKER_CONFIG = 3;

    /**
     * The option stands in the `server.properties` the broker was started with, `STATIC_BROKER_CONFIG`
     */
    public const int STATIC_BROKER_CONFIG = 4;

    /**
     * Nobody ever configured the option and the value is the built-in default, `DEFAULT_CONFIG`
     */
    public const int DEFAULT_CONFIG = 5;

    /**
     * Name of every source id, as `ConfigEntry.ConfigSource` @ 1.1.1 spells it
     *
     * @var array<int, string>
     */
    private const array NAMES = [
        self::UNKNOWN                       => 'UNKNOWN',
        self::TOPIC_CONFIG                  => 'DYNAMIC_TOPIC_CONFIG',
        self::DYNAMIC_BROKER_CONFIG         => 'DYNAMIC_BROKER_CONFIG',
        self::DYNAMIC_DEFAULT_BROKER_CONFIG => 'DYNAMIC_DEFAULT_BROKER_CONFIG',
        self::STATIC_BROKER_CONFIG          => 'STATIC_BROKER_CONFIG',
        self::DEFAULT_CONFIG                => 'DEFAULT_CONFIG',
    ];

    /**
     * This class is a namespace of constants and is never instantiated
     */
    private function __construct() {}

    /**
     * Returns the source id that travelled on the wire, folding an id this client does not know into {@see UNKNOWN}
     *
     * `ConfigSource.forId()` @ 1.1.1 does the same with everything at or above the number of ids it knows, and
     * throws for a negative one - which cannot happen here, because the field is read as a signed int8 of a broker
     * that only ever writes 0 to 5.
     */
    public static function fromWire(int $configSource): int
    {
        return isset(self::NAMES[$configSource]) ? $configSource : self::UNKNOWN;
    }

    /**
     * Returns the name of a source id, for a message or a log line
     */
    public static function nameOf(int $configSource): string
    {
        return self::NAMES[$configSource] ?? self::NAMES[self::UNKNOWN];
    }

    /**
     * Whether the source says that the value was configured for the resource itself, i.e. that it is not inherited
     *
     * `TOPIC_CONFIG` for a topic and `DYNAMIC_BROKER_CONFIG` for a broker are the two sources that a resource owns;
     * a static or a default value comes from the broker configuration, and a dynamic DEFAULT broker config from the
     * cluster.
     */
    public static function isResourceOwn(int $configSource): bool
    {
        return $configSource === self::TOPIC_CONFIG || $configSource === self::DYNAMIC_BROKER_CONFIG;
    }
}
