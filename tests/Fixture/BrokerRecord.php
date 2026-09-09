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

namespace Protocol\Kafka\Tests\Fixture;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Broker entry of a Metadata response v0, used to exercise nested objects and arrays of objects
 *
 * <pre>
 *   Broker => NodeId Host Port
 * </pre>
 */
final class BrokerRecord implements BinarySchemaInterface
{
    public int $nodeId = 0;

    public string $host = '';

    public int $port = 0;

    public static function of(int $nodeId, string $host, int $port): self
    {
        $broker         = new self();
        $broker->nodeId = $nodeId;
        $broker->host   = $host;
        $broker->port   = $port;

        return $broker;
    }

    public static function getScheme(): array
    {
        return [
            'nodeId' => BinarySchema::TYPE_INT32,
            'host'   => BinarySchema::TYPE_STRING,
            'port'   => BinarySchema::TYPE_INT32,
        ];
    }
}
