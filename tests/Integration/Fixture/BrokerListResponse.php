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

namespace Protocol\Kafka\Tests\Integration\Fixture;

use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\AbstractResponse;

/**
 * Reads the broker part of a Metadata response v0 with the typed Stream API.
 *
 * <pre>
 *   MetadataResponse => [Broker][TopicMetadata]
 *     Broker => NodeId Host Port
 * </pre>
 *
 * The topic part is intentionally left untouched, it is unpacked by the protocol classes of wave 2.
 */
final class BrokerListResponse extends AbstractResponse
{
    /**
     * @var list<array{nodeId: int, host: string, port: int}>
     */
    public array $brokers = [];

    protected function unpackBody(Stream $stream): void
    {
        $this->brokers = $stream->readArray(static fn(Stream $stream): array => [
            'nodeId' => $stream->readInt32(),
            'host'   => (string) $stream->readString(),
            'port'   => $stream->readInt32(),
        ]);
    }
}
