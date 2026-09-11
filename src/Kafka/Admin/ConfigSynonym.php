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

use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigSynonym;

/**
 * One of the places a broker looked for the value of a {@see ConfigEntry}, as KIP-226 reports them
 *
 * The fields are the ones of `org.apache.kafka.clients.admin.ConfigEntry.ConfigSynonym` @ 1.1.1 - `name`, `value`
 * and `source` - and the list of an entry is ordered by precedence: the **first** synonym is the one that won and
 * carries the value of the entry itself, everything behind it is shadowed. An entry that was read with
 * `include_synonyms = false`, or from a version 0 answer, has an empty list.
 *
 * The name of a synonym is not necessarily the name of the option: the synonyms of the topic option `retention.ms`
 * are the broker options `log.retention.ms`, `log.retention.minutes` and `log.retention.hours` (in that order), so
 * the list is what says WHY an option has the value it has.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0, v1 and v2)"
 */
final class ConfigSynonym
{
    /**
     * @param string      $name   Name of the option this value was read under, e.g. `log.retention.ms`
     * @param string|null $value  Value as text, null when the option is sensitive
     * @param int         $source Where this value comes from, one of the {@see ConfigSource} constants
     */
    public function __construct(
        public readonly string $name,
        public readonly ?string $value,
        public readonly int $source = ConfigSource::UNKNOWN
    ) {}

    /**
     * Builds the synonym from the one of a DescribeConfigs v1 answer
     */
    public static function fromResponseSynonym(DescribeConfigsResponseConfigSynonym $synonym): self
    {
        return new self($synonym->configName, $synonym->configValue, ConfigSource::fromWire($synonym->configSource));
    }
}
