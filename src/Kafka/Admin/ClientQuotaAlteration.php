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

use Protocol\Kafka\Protocol\Data\AlterClientQuotasRequestEntry;
use Protocol\Kafka\Protocol\Data\ClientQuotaOpData;

/**
 * One entity of an {@see AdminClient::alterClientQuotas()} call and the changes asked for it
 *
 * `ClientQuotaAlteration` of the Java admin client, whose nested `Op` is {@see ClientQuotaAlterationOp} here.
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0)"
 */
final class ClientQuotaAlteration
{
    /**
     * @param ClientQuotaEntity              $entity Entity whose quotas change
     * @param list<ClientQuotaAlterationOp>  $ops    Changes to apply to it
     */
    public function __construct(public readonly ClientQuotaEntity $entity, public readonly array $ops) {}

    /**
     * Returns this alteration in the shape the api puts on the wire
     */
    public function toData(): AlterClientQuotasRequestEntry
    {
        $ops = [];
        foreach ($this->ops as $op) {
            $ops[$op->key] = new ClientQuotaOpData($op->key, $op->value ?? 0.0, $op->value === null);
        }

        return new AlterClientQuotasRequestEntry($this->entity->toData(), $ops);
    }
}
