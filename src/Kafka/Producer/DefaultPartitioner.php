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

/**
 * @author Alexander.Lisachenko
 * @date   29.07.2016
 */

namespace Protocol\Kafka\Producer;

use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\PartitionMetadata;

/**
 * The default partitioning strategy:
 *
 * 1) If a partition is specified in the record, use it
 * 2) If no partition is specified but a key is present choose a partition based on a hash of the key
 * 3) If no partition or key is present choose a partition in a round-robin fashion
 *
 * The hash of the key is the 32-bit MurmurHash2 of its bytes, made positive and taken modulo the number of
 * partitions of the topic, which is what `org.apache.kafka.clients.producer.internals.DefaultPartitioner` does. A
 * record produced by this client therefore lands in the same partition as a record produced with the same key by
 * the official Java client, and log compaction and partition-local ordering keep working across both.
 *
 * @see kafka.common.utils.Utils#murmur2 @ 0.10.2.2 (clients/src/main/java/org/apache/kafka/common/utils/Utils.java)
 */
class DefaultPartitioner implements PartitionerInterface
{
    /**
     * Seed of the hash, `0x9747b28c` as in the Java implementation
     */
    public const int MURMUR2_SEED = 0x9747B28C;

    /**
     * Mixing constant 'm' of the algorithm
     */
    private const int MURMUR2_M = 0x5BD1E995;

    /**
     * Mixing shift 'r' of the algorithm
     */
    private const int MURMUR2_R = 24;

    /**
     * All the bits of a 32-bit unsigned value
     */
    private const int UINT32_MASK = 0xFFFFFFFF;

    /**
     * Counter of the round-robin distribution of the records that carry no key
     *
     * The counter starts at a random value, exactly like the `AtomicInteger` of the Java partitioner, so that the
     * short-lived producers of a PHP request do not all start writing to the same partition.
     */
    private int $counter;

    public function __construct()
    {
        $this->counter = random_int(0, self::UINT32_MASK);
    }

    /**
     * Computes the 32-bit MurmurHash2 of a byte string, bit for bit the value of `Utils.murmur2()`.
     *
     * The Java implementation works on signed 32-bit integers, so the arithmetic here is done modulo 2^32 and the
     * result is converted back into the signed range at the end.
     *
     * @return int The hash as a signed 32-bit integer
     */
    public static function murmur2(string $data): int
    {
        $length = strlen($data);
        $hash   = (self::MURMUR2_SEED ^ $length) & self::UINT32_MASK;
        $blocks = $length >> 2;

        for ($index = 0; $index < $blocks; $index++) {
            $offset = $index << 2;
            $block  = ord($data[$offset])
                | (ord($data[$offset + 1]) << 8)
                | (ord($data[$offset + 2]) << 16)
                | (ord($data[$offset + 3]) << 24);

            $block = self::multiply($block, self::MURMUR2_M);
            $block ^= $block >> self::MURMUR2_R;
            $block = self::multiply($block, self::MURMUR2_M);

            $hash = self::multiply($hash, self::MURMUR2_M) ^ $block;
        }

        // The trailing bytes fall through the cases of the switch of the Java implementation
        $tail = $length & ~3;
        switch ($length & 3) {
            case 3:
                $hash ^= ord($data[$tail + 2]) << 16;
                // no break
            case 2:
                $hash ^= ord($data[$tail + 1]) << 8;
                // no break
            case 1:
                $hash ^= ord($data[$tail]);
                $hash = self::multiply($hash, self::MURMUR2_M);
        }

        $hash ^= $hash >> 13;
        $hash = self::multiply($hash, self::MURMUR2_M);
        $hash ^= $hash >> 15;

        return $hash > 0x7FFFFFFF ? $hash - 0x100000000 : $hash;
    }

    /**
     * Drops the sign bit of a hash, the `Utils.abs()` of Kafka 0.8 and the `Utils.toPositive()` of the later lines.
     *
     * This is not the absolute value: `abs(Integer.MIN_VALUE)` is negative in Java, so the official clients mask the
     * sign bit off instead, and a client that computes the real absolute value would place some keys in a different
     * partition.
     */
    public static function toPositive(int $hash): int
    {
        return $hash & 0x7FFFFFFF;
    }

    /**
     * Compute the partition for the given record.
     *
     * A key is hashed over *all* the partitions of the topic, so that the partition of a key never changes while the
     * topic keeps its size, even when a partition currently has no leader. A record without a key goes to the next
     * available partition instead, because nothing forces it to a particular one.
     *
     * @param string      $topic   The topic name
     * @param string|null $key     The key to partition on (or null if no key)
     * @param string|null $value   The value to partition on or null
     * @param Cluster     $cluster The current cluster metadata
     *
     * @throws InvalidTopicException for a topic that the cluster metadata knows nothing about
     */
    public function partition(string $topic, ?string $key, ?string $value, Cluster $cluster): int
    {
        $partitions      = $cluster->partitionsForTopic($topic);
        $totalPartitions = count($partitions);

        if ($totalPartitions === 0) {
            throw new InvalidTopicException(['topic' => $topic, 'error' => 'Topic has no partitions at all']);
        }

        if (isset($key)) {
            return self::toPositive(self::murmur2($key)) % $totalPartitions;
        }

        // A partition without a leader can not accept a write, so the round-robin skips it
        $availablePartitionIds = [];
        foreach ($partitions as $partitionMetadata) {
            /** @var PartitionMetadata $partitionMetadata */
            if ($partitionMetadata->leader !== -1) {
                $availablePartitionIds[] = $partitionMetadata->partitionId;
            }
        }

        $nextValue = $this->counter++;
        if ($availablePartitionIds === []) {
            // No leader anywhere: the record still has to be assigned, the produce request will fail and be retried
            return self::toPositive($nextValue) % $totalPartitions;
        }

        sort($availablePartitionIds);

        return $availablePartitionIds[self::toPositive($nextValue) % count($availablePartitionIds)];
    }

    /**
     * Multiplies two 32-bit values modulo 2^32.
     *
     * A direct multiplication of two operands close to 2^32 overflows the 64-bit integers of PHP and silently turns
     * into a float, which loses the low bits that the hash is made of, so the left operand is split in two halves.
     */
    private static function multiply(int $left, int $right): int
    {
        $left  &= self::UINT32_MASK;
        $right &= self::UINT32_MASK;

        $lowResult  = ($left & 0xFFFF) * $right;
        $highResult = ((($left >> 16) * $right) & 0xFFFF) << 16;

        return ($highResult + $lowResult) & self::UINT32_MASK;
    }
}
