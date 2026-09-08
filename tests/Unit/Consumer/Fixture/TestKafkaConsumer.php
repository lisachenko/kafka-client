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

namespace Protocol\Kafka\Tests\Unit\Consumer\Fixture;

use LogicException;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Consumer\KafkaConsumer;

/**
 * A consumer that talks to a {@see FakeClient} instead of a broker.
 *
 * The consumer builds its cluster and its client lazily behind two protected methods exactly for this: everything
 * but partitionsFor() can be driven without a single socket.
 */
final class TestKafkaConsumer extends KafkaConsumer
{
    /**
     * @param array<string, mixed> $configuration Consumer options
     */
    public function __construct(private readonly FakeClient $fakeClient, array $configuration = [])
    {
        parent::__construct($configuration);
    }

    protected function getClient(): Client
    {
        return $this->fakeClient;
    }

    protected function getCluster(): Cluster
    {
        throw new LogicException('This consumer must not resolve a cluster, it talks to a fake client');
    }

    /**
     * @inheritdoc
     */
    protected function partitionsForAssignment(array $topics): array
    {
        return array_intersect_key($this->fakeClient->partitionsPerTopic, array_flip($topics));
    }
}
