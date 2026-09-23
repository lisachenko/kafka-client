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
 * One client-metrics subscription of a ListClientMetricsResources answer (key 74, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   ClientMetricsResource => Name
 *     Name => COMPACT_STRING
 * </pre>
 *
 * A name and nothing else - `ListClientMetricsResourcesResponse.json` @ 3.7.2 gives the structure one field, and
 * not even an `about` for it. The name is the entity name of a `client-metrics` configuration resource, i.e. what
 * `kafka-configs.sh --entity-type client-metrics --entity-name <name>` wrote; its contents are read with
 * DescribeConfigs on the resource type `CLIENT_METRICS`.
 *
 * @see docs/protocol/4.3.md, section "ListClientMetricsResources API (key 74, v0)"
 */
class ClientMetricsResource implements BinarySchemaInterface
{
    /**
     * Name of the `client-metrics` configuration resource this subscription lives in
     */
    public string $name;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name' => BinarySchema::TYPE_STRING,
        ];
    }
}
