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

/**
 * One client-metrics subscription of a ListClientMetricsResources answer of version 0 (Kafka 3.7, KIP-714)
 *
 * The name and nothing else: version 1 (Kafka 4.1, KIP-1142) appended the `resource_type` of
 * {@see ClientMetricsResource}, and an entry of this version is always a `client-metrics` resource, which is the
 * `"default": 16` its {@see ClientMetricsResource::$resourceType} keeps.
 *
 * @see docs/protocol/4.3.md, section "ListClientMetricsResources API (key 74, v0 and v1)"
 */
final class ClientMetricsResourceV0 extends ClientMetricsResource
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
