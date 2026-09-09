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

namespace Protocol\Kafka\Tests\Unit\Support;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Tests\Fixture\ClusterMetadataResponse;
use Protocol\Kafka\Tests\Fixture\ClusterReadinessProbe;
use Protocol\Kafka\Tests\Fixture\FakeStream;

/**
 * The readiness probe drives the integration suite, so it gets tested against canned broker answers rather than
 * against a live broker.
 *
 * @see docs/protocol/0.11.0.md, section "Cluster readiness"
 */
#[CoversClass(ClusterReadinessProbe::class)]
#[CoversClass(ClusterMetadataResponse::class)]
final class ClusterReadinessProbeTest extends TestCase
{
    /**
     * A Metadata response v0 whose broker array is empty, exactly what a freshly booted broker answers
     */
    private const string EMPTY_METADATA_RESPONSE = '0000000c'  // Size
        . '00000001'                                           // CorrelationId
        . '00000000'                                           // [Broker] - empty
        . '00000000';                                          // [TopicMetadata] - empty

    /**
     * The same response once the controller has published the metadata cache
     */
    private const string READY_METADATA_RESPONSE = '0000001b'  // Size
        . '00000001'                                           // CorrelationId
        . '00000001'                                           // [Broker] - one entry
        . '00000000'                                           // NodeId
        . '0009' . '3132372e302e302e31'                        // Host "127.0.0.1"
        . '00002384'                                           // Port 9092
        . '00000000';                                          // [TopicMetadata] - empty

    public function testBrokersAreReturnedAsSoonAsTheClusterPublishesThem(): void
    {
        $probe = new ClusterReadinessProbe($this->replay(self::READY_METADATA_RESPONSE), 5.0, 1000);

        $brokers = $probe->awaitBrokers('probe-topic');

        self::assertSame(1, $probe->getAttempts());
        self::assertSame([0], array_keys($brokers));
        self::assertSame('127.0.0.1', $brokers[0]->host);
        self::assertSame(9092, $brokers[0]->port);
    }

    public function testAnEmptyBrokerArrayIsRetriedInsteadOfBeingReportedAsAnEmptyCluster(): void
    {
        $probe = new ClusterReadinessProbe(
            $this->replay(
                self::EMPTY_METADATA_RESPONSE,
                self::EMPTY_METADATA_RESPONSE,
                self::READY_METADATA_RESPONSE
            ),
            5.0,
            1000
        );

        $brokers = $probe->awaitBrokers('probe-topic');

        self::assertSame(3, $probe->getAttempts(), 'the two empty answers have to be retried');
        self::assertSame('127.0.0.1', $brokers[0]->host);
    }

    public function testProbeAsksForATopicSoThatTheBrokerFillsItsMetadataCache(): void
    {
        $streams = [];
        $probe   = new ClusterReadinessProbe(
            function () use (&$streams): FakeStream {
                return $streams[] = new FakeStream(hex2bin(self::READY_METADATA_RESPONSE));
            },
            5.0,
            1000
        );

        $probe->awaitBrokers('probe-topic');

        // Size | ApiKey 3 | ApiVersion 0 | CorrelationId 1 | ClientId | [TopicName] with the probe topic
        $request = new StringStream($streams[0]->getWrittenBytes());
        $request->readInt32();
        self::assertSame(3, $request->readInt16(), 'the probe has to use the Metadata api key');
        self::assertSame(0, $request->readInt16());
        self::assertSame(1, $request->readInt32());
        self::assertSame(ClusterReadinessProbe::PROBE_TOPIC_PREFIX, $request->readString());
        self::assertSame(1, $request->readInt32(), 'exactly one topic must be requested');
        self::assertSame('probe-topic', $request->readString());
    }

    public function testAConnectionErrorWhileBootingIsRetried(): void
    {
        $attempt = 0;
        $probe   = new ClusterReadinessProbe(
            function () use (&$attempt): FakeStream {
                $attempt++;

                // The first attempt hits a broker that closes the connection before answering
                return new FakeStream($attempt === 1 ? '' : hex2bin(self::READY_METADATA_RESPONSE));
            },
            5.0,
            1000
        );

        self::assertNotEmpty($probe->awaitBrokers('probe-topic'));
        self::assertSame(2, $probe->getAttempts());
    }

    public function testProbeGivesUpAfterTheTimeout(): void
    {
        $probe = new ClusterReadinessProbe($this->replay(self::EMPTY_METADATA_RESPONSE), 0.05, 1000);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/did not publish any broker/');
        $probe->awaitBrokers('probe-topic');
    }

    /**
     * Builds a stream factory that replays the given hex frames, repeating the last one once they run out
     */
    private function replay(string ...$hexFrames): \Closure
    {
        $index = 0;

        return function () use ($hexFrames, &$index): FakeStream {
            $frame = $hexFrames[min($index, count($hexFrames) - 1)];
            $index++;

            return new FakeStream(hex2bin($frame));
        };
    }
}
