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
 * One entity of an AlterClientQuotas request and the changes asked for it (key 49, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   EntryData => Entity Ops
 *     Entity => ARRAY of {@see ClientQuotaEntityData}
 *     Ops    => ARRAY of {@see ClientQuotaOpData}
 * </pre>
 *
 * Every entry of a request is applied on its own and is answered on its own, so one refused entity does not stop
 * the others.
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0 and v1)"
 */
class AlterClientQuotasRequestEntry implements BinarySchemaInterface
{
    /**
     * Parts of the entity to change, indexed by the entity type
     *
     * @var array<string, ClientQuotaEntityData>
     */
    public array $entity;

    /**
     * Changes to apply to it, indexed by the quota name
     *
     * @var array<string, ClientQuotaOpData>
     */
    public array $ops;

    /**
     * @param array<string, ClientQuotaEntityData> $entity Parts of the entity, by entity type
     * @param array<string, ClientQuotaOpData>     $ops    Changes, by quota name
     */
    public function __construct(array $entity, array $ops)
    {
        $this->entity = $entity;
        $this->ops    = $ops;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'entity' => ['entityType' => ClientQuotaEntityData::class],
            'ops'    => ['key' => ClientQuotaOpData::class],
        ];
    }
}
