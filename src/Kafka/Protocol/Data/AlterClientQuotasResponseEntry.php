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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of one entity of an AlterClientQuotas answer (key 49, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   EntryData => ErrorCode ErrorMessage Entity
 *     ErrorCode    => INT16
 *     ErrorMessage => NULLABLE_STRING
 *     Entity       => ARRAY of {@see ClientQuotaEntityData}
 * </pre>
 *
 * **The error comes before the entity it belongs to**, so an answer is read by walking it and matching the entity
 * against the request; the broker answers one entry per entry of the request, in the order it received them.
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0 and v1)"
 */
class AlterClientQuotasResponseEntry implements BinarySchemaInterface
{
    /**
     * Error of this entity, 0 when every change of it was applied
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Parts of the entity this result belongs to, indexed by the entity type
     *
     * @var array<string, ClientQuotaEntityData>
     */
    public array $entity;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
            'entity'       => ['entityType' => ClientQuotaEntityData::class],
        ];
    }
}
