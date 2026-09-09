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

use Protocol\Kafka\Protocol\Request\AbstractResponse;

/**
 * The broker part of a Metadata response v0, declared as a scheme.
 *
 * <pre>
 *   MetadataResponse => [Broker][TopicMetadata]
 *     Broker => NodeId Host Port
 * </pre>
 *
 * The topic part is deliberately left out: this fixture only has to tell whether the broker has published its
 * metadata yet, which is what {@see ClusterReadinessProbe} asks it for with a version 0 request. Everything the
 * later versions of the api add - the rack of a broker, the cluster id, the controller id - sits AFTER the broker
 * array, so a version 1 or 2 answer would be read past the last field this scheme has; that is harmless, because
 * {@see \Protocol\Kafka\Protocol\AbstractProtocolMessage::unpack()} reads the whole frame off the connection first,
 * but the probe asks with version 0 anyway.
 */
final class ClusterMetadataResponse extends AbstractResponse
{
    /**
     * @var array<int, BrokerRecord>
     */
    public array $brokers = [];

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'brokers' => ['nodeId' => BrokerRecord::class],
        ];
    }
}
