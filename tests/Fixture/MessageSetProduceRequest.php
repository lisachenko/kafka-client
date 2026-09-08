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

use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\AbstractRequest;

/**
 * Produce request v0 that carries a ready-made message set, compressed or not.
 *
 * <pre>
 *   ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
 *     RequiredAcks   => int16
 *     Timeout        => int32
 *     Partition      => int32
 *     MessageSetSize => int32
 * </pre>
 *
 * The production {@see \Protocol\Kafka\Protocol\Request\ProduceRequest} builds the message set out of records with
 * the default codec; this fixture exists so that the tests can put an arbitrary {@see MessageSet} on the wire.
 */
final class MessageSetProduceRequest extends AbstractRequest
{
    /**
     * @param array<string, array<int, MessageSet>> $topicPartitionMessageSets Message set per topic and partition
     */
    public function __construct(
        private readonly array $topicPartitionMessageSets,
        private readonly int $requiredAcks = 1,
        private readonly int $timeout = 5000,
        string $clientId = '',
        int $correlationId = 0,
    ) {
        parent::__construct(ApiKeys::PRODUCE, $clientId, $correlationId);
    }

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload = parent::packPayload();
        $payload .= pack('nNN', $this->requiredAcks, $this->timeout, count($this->topicPartitionMessageSets));

        foreach ($this->topicPartitionMessageSets as $topic => $partitions) {
            $topicLength = strlen($topic);
            $payload .= pack("na{$topicLength}N", $topicLength, $topic, count($partitions));
            foreach ($partitions as $partition => $messageSet) {
                $buffer = $messageSet->toBuffer();
                $payload .= pack('NN', $partition, strlen($buffer)) . $buffer;
            }
        }

        return $payload;
    }
}
