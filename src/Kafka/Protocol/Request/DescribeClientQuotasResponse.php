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
use Protocol\Kafka\Protocol\Data\DescribeClientQuotasResponseEntry;

/**
 * DescribeClientQuotas response object, version 0 (key 48, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   DescribeClientQuotas Response (Version: 0) => throttle_time_ms error_code error_message [entries]
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     error_message    => NULLABLE_STRING
 *     entries          => entity [values]          (the array itself is NULLABLE)
 *       entity => entity_type entity_name
 *       values => key value
 * </pre>
 *
 * There is no per-entity error: either the filter was understood and every matching entity is in `entries`, or
 * the top-level code says why nothing is. **The entry array is nullable** and is `null` exactly when the top-level
 * code is not 0 - `DescribeClientQuotasResponse.fromException()` @ 2.8.2 builds the error answer with a null
 * array - while a filter that simply matched nothing answers an **empty** one.
 *
 * The codes measured on the container are **0**, **42** (InvalidRequest) for a filter this broker cannot make
 * sense of - an unknown entity type, an unknown match type, a duplicated entity type - and **35**
 * (UnsupportedVersionException) when a request reaches a broker that has no quota cache to answer from.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0)"
 */
class DescribeClientQuotasResponse extends AbstractResponse
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
     * Error code of the request as a whole, 0 when the filter was understood
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Entities that matched the filter, null when the request was refused
     *
     * @var list<DescribeClientQuotasResponseEntry>|null
     */
    public ?array $entries = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
            'entries'        => [
                DescribeClientQuotasResponseEntry::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
        ];
    }
}
