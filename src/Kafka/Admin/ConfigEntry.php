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

use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntry;

/**
 * One configuration option of a {@see Config}, as a DescribeConfigs answer describes it
 *
 * The fields are the ones of `org.apache.kafka.clients.admin.ConfigEntry` @ 0.11.0.3 - `name`, `value`,
 * `isDefault`, `isSensitive`, `isReadOnly` - and their meaning is the one `AdminManager.describeConfigs` gives
 * them:
 *
 *  - `value` is the option as text, and it is **null** for a sensitive option: the broker never sends the value of
 *    an option whose type is `PASSWORD`;
 *  - `isDefault` is true when nothing was configured for this resource and the value shown is the one the broker
 *    defaults imply - for a topic, that the option is not in its ZooKeeper node;
 *  - `isReadOnly` is true for every option of a **broker** resource, because a 0.11 broker cannot change its own
 *    configuration at runtime, and false for every option of a topic.
 *
 * @see docs/protocol/0.11.0.md, section "DescribeConfigs API (key 32, v0)"
 */
final class ConfigEntry
{
    /**
     * @param string      $name        Name of the option, e.g. `retention.ms`
     * @param string|null $value       Value of the option as text, null when it is sensitive
     * @param bool        $isDefault   Whether nothing was configured for this resource
     * @param bool        $isSensitive Whether the option holds a secret, whose value the broker never sends
     * @param bool        $isReadOnly  Whether the option cannot be changed with the AlterConfigs api
     */
    public function __construct(
        public readonly string $name,
        public readonly ?string $value,
        public readonly bool $isDefault = false,
        public readonly bool $isSensitive = false,
        public readonly bool $isReadOnly = false
    ) {}

    /**
     * Builds the entry from the one of a DescribeConfigs answer
     */
    public static function fromResponseEntry(DescribeConfigsResponseConfigEntry $entry): self
    {
        return new self(
            $entry->configName,
            $entry->configValue,
            $entry->isDefault,
            $entry->isSensitive,
            $entry->readOnly
        );
    }
}
