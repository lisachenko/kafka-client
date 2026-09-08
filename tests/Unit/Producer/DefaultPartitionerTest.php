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

namespace Protocol\Kafka\Tests\Unit\Producer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Producer\DefaultPartitioner;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\ClusterFixture;

/**
 * Verifies that the default partitioner places a record exactly where the official Java client places it.
 *
 * The expected hashes were produced by `org.apache.kafka.common.utils.Utils.murmur2()` of the
 * `kafka-clients` jar that ships with the broker this suite runs against, so a green run here means that a
 * key written by this client and the same key written by the Java client end up in the same partition.
 */
#[CoversClass(DefaultPartitioner::class)]
final class DefaultPartitionerTest extends TestCase
{
    /**
     * Name of the topic that the tests of this class partition on
     */
    private const string TOPIC = 'partitioner-topic';

    protected function tearDown(): void
    {
        ClusterFixture::cleanUp();
    }

    /**
     * Hash of a key, as the Java implementation computes it
     *
     * @return \Generator<string, array{0: string, 1: int}>
     */
    public static function javaMurmur2Vectors(): \Generator
    {
        yield 'empty string'    => ['', 275646681];
        yield 'one byte'        => ['a', -1563381124];
        yield 'two bytes'       => ['ab', 316155434];
        yield 'three bytes'     => ['abc', 479470107];
        yield 'nine bytes'      => ['123456789', -1822237082];
        yield 'hello'           => ['hello', 2132663229];
        yield 'kafka'           => ['kafka', -798503068];
        yield 'key-0'           => ['key-0', 29210041];
        yield 'key-1'           => ['key-1', 193331640];
        yield 'key-2'           => ['key-2', 852269702];
        yield 'user-42'         => ['user-42', 1459644460];
        yield 'a whole pangram' => ['The quick brown fox jumps over the lazy dog', 495243318];
        yield 'a null byte'     => ["\x00\x20", 1121897742];
        yield 'every byte'      => [self::everyByte(), -1948347274];
    }

    #[DataProvider('javaMurmur2Vectors')]
    public function testTheHashIsTheOneOfTheJavaClient(string $key, int $expectedHash): void
    {
        self::assertSame($expectedHash, DefaultPartitioner::murmur2($key));
    }

    public function testTheHashIsMadePositiveByDroppingItsSignBitOnly(): void
    {
        // The Java clients mask the sign bit off instead of taking the absolute value, which is negative for the
        // smallest integer, and a client that used abs() would place those keys elsewhere
        self::assertSame(0, DefaultPartitioner::toPositive(-2147483648));
        self::assertSame(2147483647, DefaultPartitioner::toPositive(-1));
        self::assertSame(584102524, DefaultPartitioner::toPositive(DefaultPartitioner::murmur2('a')));
        self::assertSame(275646681, DefaultPartitioner::toPositive(DefaultPartitioner::murmur2('')));
    }

    /**
     * Partition that a key gets in a topic of 1000 partitions, from the Java client
     *
     * @return \Generator<string, array{0: string, 1: int}>
     */
    public static function javaPartitionVectors(): \Generator
    {
        yield 'empty string' => ['', 681];
        yield 'one byte'     => ['a', 524];
        yield 'two bytes'    => ['ab', 434];
        yield 'three bytes'  => ['abc', 107];
        yield 'nine bytes'   => ['123456789', 566];
        yield 'a null byte'  => ["\x00\x20", 742];
    }

    #[DataProvider('javaPartitionVectors')]
    public function testAKeyGoesToThePartitionOfTheJavaClient(string $key, int $expectedPartition): void
    {
        $cluster = ClusterFixture::withPartitions([self::TOPIC => array_fill(0, 1000, 1)]);

        self::assertSame(
            $expectedPartition,
            new DefaultPartitioner()->partition(self::TOPIC, $key, 'any value', $cluster)
        );
    }

    public function testAKeyAlwaysGoesToTheSamePartition(): void
    {
        $cluster     = $this->clusterOfThreePartitions();
        $partitioner = new DefaultPartitioner();

        $firstPartition = $partitioner->partition(self::TOPIC, 'user-42', 'first value', $cluster);
        for ($attempt = 0; $attempt < 10; $attempt++) {
            self::assertSame(
                $firstPartition,
                $partitioner->partition(self::TOPIC, 'user-42', "value #{$attempt}", $cluster),
                'The partition of a key may not depend on the value of the record or on the call count'
            );
        }
        // toPositive(murmur2('user-42')) % 3, computed by the Java client
        self::assertSame(1, $firstPartition);
    }

    public function testAKeyIsHashedOverEveryPartitionEvenWithoutALeader(): void
    {
        // The partition of a key may not move while a leader election is going on, otherwise the ordering of the
        // records of that key would be lost
        $withLeaders    = ClusterFixture::withPartitions([self::TOPIC => [0 => 1, 1 => 1, 2 => 1]]);
        $withoutLeaders = ClusterFixture::withPartitions([self::TOPIC => [0 => 1, 1 => -1, 2 => -1]]);

        $partitioner = new DefaultPartitioner();
        foreach (['key-0', 'key-1', 'key-2', 'user-42'] as $key) {
            self::assertSame(
                $partitioner->partition(self::TOPIC, $key, null, $withLeaders),
                $partitioner->partition(self::TOPIC, $key, null, $withoutLeaders)
            );
        }
    }

    public function testARecordWithoutAKeyIsSpreadOverThePartitionsInARoundRobinFashion(): void
    {
        $cluster     = $this->clusterOfThreePartitions();
        $partitioner = new DefaultPartitioner();

        $partitions = [];
        for ($record = 0; $record < 9; $record++) {
            $partitions[] = $partitioner->partition(self::TOPIC, null, "value #{$record}", $cluster);
        }

        // Three full rounds over the three partitions, whichever one the random counter started at
        self::assertSame([3, 3, 3], array_values(array_count_values($partitions)));
        self::assertSame(array_slice($partitions, 0, 3), array_slice($partitions, 3, 3));
        self::assertSame(array_slice($partitions, 0, 3), array_slice($partitions, 6, 3));
    }

    public function testARecordWithoutAKeySkipsThePartitionsThatHaveNoLeader(): void
    {
        $cluster     = ClusterFixture::withPartitions([self::TOPIC => [0 => -1, 1 => 1, 2 => -1, 3 => 1]]);
        $partitioner = new DefaultPartitioner();

        $partitions = [];
        for ($record = 0; $record < 20; $record++) {
            $partitions[] = $partitioner->partition(self::TOPIC, null, 'a value', $cluster);
        }

        $usedPartitions = array_values(array_unique($partitions));
        sort($usedPartitions);

        self::assertSame([1, 3], $usedPartitions);
        self::assertCount(10, array_keys($partitions, 1, true));
        self::assertCount(10, array_keys($partitions, 3, true));
    }

    public function testARecordWithoutAKeyStillGetsAPartitionWhenNoLeaderIsKnownAtAll(): void
    {
        // The record has to be buffered somewhere; the produce request will fail and the retry will find a leader
        $cluster     = ClusterFixture::withPartitions([self::TOPIC => [0 => -1, 1 => -1]]);
        $partitioner = new DefaultPartitioner();

        for ($record = 0; $record < 4; $record++) {
            self::assertContains($partitioner->partition(self::TOPIC, null, 'a value', $cluster), [0, 1]);
        }
    }

    public function testAnUnknownTopicIsRejected(): void
    {
        $cluster = $this->clusterOfThreePartitions();

        $this->expectException(InvalidTopicException::class);

        new DefaultPartitioner()->partition('there-is-no-such-topic', 'a key', 'a value', $cluster);
    }

    /**
     * Returns a string of all the 256 byte values, which exercises the tail handling of the hash
     */
    private static function everyByte(): string
    {
        return implode('', array_map(chr(...), range(0, 255)));
    }

    /**
     * Returns a cluster whose single topic has three partitions, all of them led by the only broker
     */
    private function clusterOfThreePartitions(): Cluster
    {
        return ClusterFixture::withPartitions([self::TOPIC => [0 => 1, 1 => 1, 2 => 1]]);
    }
}
