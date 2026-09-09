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

namespace Protocol\Kafka\Tests\Unit\Fixture;

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;

/**
 * A client that opens {@see Client::produceRecords()} to a test.
 *
 * The producer state of KIP-98 - the producer id, its epoch, the sequence number of every topic-partition and the
 * transactional id - is bookkeeping that the idempotent and the transactional producer own; the low-level client
 * only carries the four values into the record batch and into the Produce request. This subclass makes that seam
 * visible without giving `Client` a public method that nothing but a test would call.
 */
final class TransactionalTestClient extends Client
{
    /**
     * Produces with the producer state that an idempotent or transactional producer would hold
     *
     * @param array<string, array<int, iterable<Record|string|\Stringable>>> $topicPartitionMessages
     * @param array<string, array<int, int>>                                 $baseSequences
     *
     * @return array<string, array<int, ProduceResponsePartition>>
     */
    public function produceRecordsWith(
        array $topicPartitionMessages,
        int $producerId,
        int $producerEpoch,
        array $baseSequences = [],
        ?string $transactionalId = null
    ): array {
        return $this->produceRecords(
            $topicPartitionMessages,
            $producerId,
            $producerEpoch,
            $baseSequences,
            $transactionalId
        );
    }
}
