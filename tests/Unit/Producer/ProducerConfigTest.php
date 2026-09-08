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
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Producer\DefaultPartitioner;
use Protocol\Kafka\Producer\ProducerConfig;

/**
 * Verifies the defaults of the producer and the resolution of the `compression.type` option
 */
#[CoversClass(ProducerConfig::class)]
final class ProducerConfigTest extends TestCase
{
    public function testTheDefaultsAreTheOnesOfTheOfficialProducer(): void
    {
        $configuration = ProducerConfig::getDefaultConfiguration();

        self::assertSame(DefaultPartitioner::class, $configuration[ProducerConfig::PARTITIONER_CLASS]);
        self::assertSame(1, $configuration[ProducerConfig::ACKS]);
        self::assertSame(2000, $configuration[ProducerConfig::TIMEOUT_MS]);
        self::assertSame(0, $configuration[ProducerConfig::RETRIES]);
        self::assertSame(0, $configuration[ProducerConfig::BATCH_SIZE]);
        self::assertSame('none', $configuration[ProducerConfig::COMPRESSION_TYPE]);
        self::assertSame(0, $configuration[ProducerConfig::LINGER_MS]);
        self::assertSame(1048576, $configuration[ProducerConfig::MAX_REQUEST_SIZE]);
    }

    public function testTheGeneralClientOptionsAreCarriedOver(): void
    {
        $configuration = ProducerConfig::getDefaultConfiguration();

        self::assertSame('PHP/Kafka', $configuration[ClientConfig::CLIENT_ID]);
        self::assertSame(100, $configuration[ClientConfig::RETRY_BACKOFF_MS]);
        self::assertArrayHasKey(ClientConfig::BOOTSTRAP_SERVERS, $configuration);
    }

    public function testKafka08HasNoTransactionalDelivery(): void
    {
        // The transactional.id of the later protocol lines arrived with Kafka 0.11
        self::assertArrayNotHasKey('transactional.id', ProducerConfig::getDefaultConfiguration());
    }

    /**
     * Codec of every value that the `compression.type` option accepts
     *
     * @return \Generator<string, array{0: string|int, 1: int}>
     */
    public static function compressionTypes(): \Generator
    {
        yield 'none'            => ['none', CompressionCodec::NONE];
        yield 'gzip'            => ['gzip', CompressionCodec::GZIP];
        yield 'snappy'          => ['snappy', CompressionCodec::SNAPPY];
        yield 'upper case'      => ['GZIP', CompressionCodec::GZIP];
        yield 'padded'          => ["  snappy\n", CompressionCodec::SNAPPY];
        yield 'the codec value' => [CompressionCodec::GZIP, CompressionCodec::GZIP];
    }

    #[DataProvider('compressionTypes')]
    public function testTheCompressionTypeIsResolvedIntoItsCodec(string|int $compressionType, int $expected): void
    {
        self::assertSame($expected, ProducerConfig::compressionCodec($compressionType));
    }

    public function testAnUnsupportedCompressionTypeIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        // 0.8.2.2 knows the lz4 codec, but this client neither writes nor reads it
        $this->expectExceptionMessage('none, gzip, snappy');

        ProducerConfig::compressionCodec('lz4');
    }

    public function testAnUnsupportedCodecValueIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        ProducerConfig::compressionCodec(3);
    }
}
