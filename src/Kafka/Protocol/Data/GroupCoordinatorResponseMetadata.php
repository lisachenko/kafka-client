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
 * GroupCoordinator response data
 *
 * The coordinator broker of a consumer group, as returned by the ConsumerMetadata API (key 10) of Kafka 0.8.2.
 *
 * <pre>
 *   CoordinatorId   => int32
 *   CoordinatorHost => string
 *   CoordinatorPort => int32
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "GroupCoordinator API (key 10, v0 and v1)"
 */
class GroupCoordinatorResponseMetadata implements BinarySchemaInterface
{
    /**
     * The broker id.
     */
    public int $nodeId;

    /**
     * The hostname of the broker.
     */
    public string $host;

    /**
     * The port on which the broker accepts requests.
     */
    public int $port;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'nodeId' => BinarySchema::TYPE_INT32,
            'host'   => BinarySchema::TYPE_STRING,
            'port'   => BinarySchema::TYPE_INT32,
        ];
    }
}
