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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\AlterClientQuotasResponseEntry;

/**
 * AlterClientQuotas response object, version 0 (key 49, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   AlterClientQuotas Response (Version: 0) => throttle_time_ms [entries]
 *     throttle_time_ms => INT32
 *     entries          => error_code error_message entity
 *       error_code    => INT16
 *       error_message => NULLABLE_STRING
 *       entity        => entity_type entity_name
 * </pre>
 *
 * **There is no top-level error code**: every entity of the request gets an entry of its own, and the error stands
 * in front of the entity it belongs to. The codes measured on the container are **0**, **42** (InvalidRequest) for
 * an entity or a quota name the broker does not know - an unknown entity type, an unknown quota key, a value that
 * is out of range - and **41** (NotController) for a request that reached a broker which does not write the quota
 * store of the cluster.
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0)"
 */
class AlterClientQuotasResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every entity of the request, in the order the request named them
     *
     * @var list<AlterClientQuotasResponseEntry>
     */
    public array $entries = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'entries'        => [AlterClientQuotasResponseEntry::class],
        ];
    }
}
