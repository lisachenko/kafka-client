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

namespace Protocol\Kafka\Admin;

use InvalidArgumentException;
use Protocol\Kafka\Common\TopicPartition;

/**
 * One replica of one partition on one broker: the thing {@see AdminClient::alterReplicaLogDirs()} moves
 *
 * `org.apache.kafka.common.TopicPartitionReplica` of the Java client, added by KIP-113 together with the two JBOD
 * apis. A {@see TopicPartition} names a partition of the cluster; this class names **one copy** of it, on the
 * broker that holds it - which is what a log directory belongs to, since every broker has its own `log.dirs`.
 *
 * <code>
 *   $admin->alterReplicaLogDirs([
 *       TopicPartitionReplica::of('events', 0, 1)->key() => '/mnt/disk-2/kafka-logs',
 *   ]);
 * </code>
 *
 * PHP cannot use an object as an array key, so a map of replicas is keyed by {@see self::key()}: the Java
 * `toString()` of the class, `topic-partition-brokerId`. {@see self::fromKey()} reads it back by taking the **last
 * two** dash-separated fields as the partition and the broker id, which is unambiguous even for a topic name that
 * contains dashes itself.
 *
 * @see docs/protocol/2.8.md, section "AlterReplicaLogDirs API (key 34, v0 to v2)"
 */
final class TopicPartitionReplica
{
    /**
     * @param string $topic     Name of the topic
     * @param int    $partition Id of the partition
     * @param int    $brokerId  Node id of the broker that holds this copy of the partition
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly int $brokerId,
    ) {}

    /**
     * Names the replica of the given partition on the given broker
     */
    public static function of(string $topic, int $partition, int $brokerId): self
    {
        return new self($topic, $partition, $brokerId);
    }

    /**
     * Returns the key this replica is addressed by in the arguments and results of {@see AdminClient}
     *
     * The string is the `toString()` of the Java class: the topic name, the partition id and the broker id, joined
     * by dashes - `events-0-1`.
     */
    public function key(): string
    {
        return "{$this->topic}-{$this->partition}-{$this->brokerId}";
    }

    /**
     * Rebuilds the replica that {@see self::key()} produced
     *
     * The last two fields of the key are the partition and the broker id, and everything before them is the topic
     * name, so a topic that contains dashes survives the round trip.
     *
     * @throws InvalidArgumentException If the key does not end in a partition id and a broker id
     */
    public static function fromKey(string $key): self
    {
        if (preg_match('/^(?P<topic>.+)-(?P<partition>\d+)-(?P<brokerId>-?\d+)$/', $key, $fields) !== 1) {
            throw new InvalidArgumentException(
                "The replica key '{$key}' does not end in a partition id and a broker id"
            );
        }

        return new self($fields['topic'], (int) $fields['partition'], (int) $fields['brokerId']);
    }

    /**
     * Returns the partition of the cluster this replica is a copy of
     */
    public function topicPartition(): TopicPartition
    {
        return new TopicPartition($this->topic, $this->partition);
    }

    public function equals(self $other): bool
    {
        return $this->topic === $other->topic
            && $this->partition === $other->partition
            && $this->brokerId === $other->brokerId;
    }

    public function __toString(): string
    {
        return $this->key();
    }
}
