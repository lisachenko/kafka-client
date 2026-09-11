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

use Protocol\Kafka\Protocol\Data\ClientQuotaComponentData;

/**
 * One component of a {@see ClientQuotaFilter}: an entity type and how its name is matched
 *
 * `ClientQuotaFilterComponent` of the Java admin client, with its three factory methods.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0)"
 */
final class ClientQuotaFilterComponent
{
    private function __construct(
        public readonly string $entityType,
        public readonly int $matchType,
        public readonly ?string $match
    ) {}

    /**
     * Matches the entity of this type whose name is exactly $name
     */
    public static function ofEntity(string $entityType, string $name): self
    {
        return new self($entityType, ClientQuotaComponentData::MATCH_TYPE_EXACT, $name);
    }

    /**
     * Matches the `<default>` entity of this type
     */
    public static function ofDefaultEntity(string $entityType): self
    {
        return new self($entityType, ClientQuotaComponentData::MATCH_TYPE_DEFAULT, null);
    }

    /**
     * Matches every entity of this type that carries a name of its own
     */
    public static function ofEntityType(string $entityType): self
    {
        return new self($entityType, ClientQuotaComponentData::MATCH_TYPE_SPECIFIED, null);
    }

    /**
     * Returns this component in the shape the api puts on the wire
     */
    public function toData(): ClientQuotaComponentData
    {
        return new ClientQuotaComponentData($this->entityType, $this->matchType, $this->match);
    }
}
