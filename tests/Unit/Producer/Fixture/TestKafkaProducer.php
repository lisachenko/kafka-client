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

namespace Protocol\Kafka\Tests\Unit\Producer\Fixture;

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Producer\KafkaProducer;

/**
 * A producer that sends its batches to a client the test controls instead of to a broker
 */
final class TestKafkaProducer extends KafkaProducer
{
    /**
     * @param array<string, mixed> $configuration Producer options
     * @param Client               $fakeClient    Client that answers the produce requests of this producer
     */
    public function __construct(array $configuration, private readonly Client $fakeClient)
    {
        parent::__construct($configuration);
    }

    /**
     * @inheritdoc
     */
    protected function createClient(Cluster $cluster, array $configuration): Client
    {
        return $this->fakeClient;
    }
}
