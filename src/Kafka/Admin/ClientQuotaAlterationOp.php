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

use Protocol\Kafka\Protocol\Data\ClientQuotaValueData;

/**
 * One change to one quota of a {@see ClientQuotaAlteration}: a new value, or `null` to remove the quota
 *
 * `ClientQuotaAlteration.Op` of the Java admin client, whose `value` is a nullable `Double` for exactly the same
 * reason: the api has no separate "unset" operation, it has a `remove` flag next to a value the broker ignores.
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0)"
 */
final class ClientQuotaAlterationOp
{
    /**
     * Bytes per second a producer of the entity may write
     */
    public const string KEY_PRODUCER_BYTE_RATE = ClientQuotaValueData::KEY_PRODUCER_BYTE_RATE;

    /**
     * Bytes per second a consumer of the entity may read
     */
    public const string KEY_CONSUMER_BYTE_RATE = ClientQuotaValueData::KEY_CONSUMER_BYTE_RATE;

    /**
     * Percentage of one request-handler thread the entity may use (KIP-124)
     */
    public const string KEY_REQUEST_PERCENTAGE = ClientQuotaValueData::KEY_REQUEST_PERCENTAGE;

    /**
     * Partition mutations per second the entity may ask the controller for (KIP-599)
     */
    public const string KEY_CONTROLLER_MUTATION_RATE = ClientQuotaValueData::KEY_CONTROLLER_MUTATION_RATE;

    /**
     * @param string     $key   Name of the quota, one of the KEY_* constants
     * @param float|null $value New value, or null to remove the quota
     */
    public function __construct(public readonly string $key, public readonly ?float $value) {}

    /**
     * Sets a quota to a value
     */
    public static function set(string $key, float $value): self
    {
        return new self($key, $value);
    }

    /**
     * Removes a quota, so that the entity falls back to the default one
     */
    public static function remove(string $key): self
    {
        return new self($key, null);
    }
}
