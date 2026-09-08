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

namespace Protocol\Kafka\Consumer;

/**
 * The committed position of a topic-partition, together with the metadata the client keeps next to it.
 *
 * The metadata is opaque to the broker: it stores the string and hands it back with the next OffsetFetch. Kafka
 * 0.8.2.2 rejects a commit whose metadata is longer than `offset.metadata.max.bytes` (4096 by default) with the
 * error code 12, OffsetMetadataTooLarge.
 */
final class OffsetAndMetadata implements \Stringable
{
    /**
     * @param int         $offset   The offset to commit for a topic-partition
     * @param string|null $metadata Any associated metadata the client wants the broker to keep, or null for none
     */
    public function __construct(
        public readonly int $offset,
        public readonly ?string $metadata = null
    ) {}

    public function __toString(): string
    {
        return $this->metadata === null
            ? "OffsetAndMetadata{offset={$this->offset}}"
            : "OffsetAndMetadata{offset={$this->offset}, metadata='{$this->metadata}'}";
    }
}
