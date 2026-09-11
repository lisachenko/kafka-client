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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One quota entity of a DescribeClientQuotas answer, with everything that is configured for it (key 48, Kafka 2.6)
 *
 * <pre>
 *   EntryData => Entity Values
 *     Entity => ARRAY of {@see ClientQuotaEntityData}
 *     Values => ARRAY of {@see ClientQuotaValueData}
 * </pre>
 *
 * The entity is an array because a quota can be attached to a combination of `user` and `client-id`; every entry
 * of the answer carries **every** quota of that entity, not only the ones the filter named.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0 and v1)"
 */
class DescribeClientQuotasResponseEntry implements BinarySchemaInterface
{
    /**
     * Parts of the entity this entry describes, indexed by the entity type
     *
     * @var array<string, ClientQuotaEntityData>
     */
    public array $entity;

    /**
     * Quotas configured for that entity, indexed by the quota name
     *
     * @var array<string, ClientQuotaValueData>
     */
    public array $values;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'entity' => ['entityType' => ClientQuotaEntityData::class],
            'values' => ['key' => ClientQuotaValueData::class],
        ];
    }
}
