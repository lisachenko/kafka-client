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

namespace Protocol\Kafka\Common;

/**
 * Identifies a single partition of a single topic, e.g. for seeking, pausing or resuming consumption.
 */
final class TopicPartition
{
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
    ) {}

    public function equals(self $other): bool
    {
        return $this->topic === $other->topic && $this->partition === $other->partition;
    }

    public function __toString(): string
    {
        return "{$this->topic}-{$this->partition}";
    }
}
