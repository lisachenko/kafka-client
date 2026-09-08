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
 * The topic part is deliberately left out: it is unpacked by the protocol classes of wave 2, this fixture only has
 * to tell whether the broker has published its metadata yet.
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
