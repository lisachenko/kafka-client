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

use Protocol\Kafka\Consumer\ConsumerRebalanceListener;

/**
 * A rebalance listener that only writes down what it was told, in the order it was told.
 */
final class RecordingRebalanceListener implements ConsumerRebalanceListener
{
    /**
     * Calls this listener received, as ['revoked'|'assigned', partitions]
     *
     * @var list<array{string, array<string, list<int>>}>
     */
    public array $calls = [];

    public function onPartitionsRevoked(array $partitions): void
    {
        $this->calls[] = ['revoked', $partitions];
    }

    public function onPartitionsAssigned(array $partitions): void
    {
        $this->calls[] = ['assigned', $partitions];
    }
}
