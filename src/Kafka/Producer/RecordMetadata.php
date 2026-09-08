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

namespace Protocol\Kafka\Producer;

/**
 * The metadata for a record that has been acknowledged by the broker.
 */
final class RecordMetadata
{
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly int $offset,
        public readonly ?int $timestamp = null,
    ) {}

    public function __toString(): string
    {
        return "{$this->topic}-{$this->partition}@{$this->offset}";
    }
}
