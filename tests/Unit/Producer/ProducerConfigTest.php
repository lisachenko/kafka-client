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
use Protocol\Kafka\Common\Record\Message;
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
        // zstd is the codec of Kafka 2.1 and the message format v2, not of this protocol line
        $this->expectExceptionMessage('none, gzip, snappy, lz4');

        ProducerConfig::compressionCodec('zstd');
    }

    public function testAnUnsupportedCodecValueIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        ProducerConfig::compressionCodec(4);
    }

    /**
     * @return iterable<string, array{0: string|int, 1: int}>
     */
    public static function messageFormatVersions(): iterable
    {
        yield 'the default of the producer' => [ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0, Message::MAGIC_V1];
        yield '0.10.0'                      => ['0.10.0', Message::MAGIC_V1];
        yield '0.10.2'                      => ['0.10.2', Message::MAGIC_V1];
        yield 'the full release name'       => ['0.10.2-IV0', Message::MAGIC_V1];
        yield '0.9.0'                       => [ProducerConfig::MESSAGE_FORMAT_VERSION_0_9_0, Message::MAGIC_V0];
        yield '0.9.0.1'                     => ['0.9.0.1', Message::MAGIC_V0];
        yield '0.8.2'                       => ['0.8.2', Message::MAGIC_V0];
        yield 'the magic byte itself'       => [Message::MAGIC_V0, Message::MAGIC_V0];
        yield 'the magic byte of v1'        => [Message::MAGIC_V1, Message::MAGIC_V1];
    }

    #[DataProvider('messageFormatVersions')]
    public function testTheMessageFormatVersionIsResolvedIntoItsMagicByte(string|int $version, int $expected): void
    {
        self::assertSame($expected, ProducerConfig::messageFormatMagic($version));
    }

    public function testTheProducerWritesMessageFormatV1ByDefault(): void
    {
        $configuration = ProducerConfig::getDefaultConfiguration();

        self::assertSame(
            ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0,
            $configuration[ProducerConfig::MESSAGE_FORMAT_VERSION]
        );
        self::assertSame(
            Message::MAGIC_V1,
            ProducerConfig::messageFormatMagic($configuration[ProducerConfig::MESSAGE_FORMAT_VERSION])
        );
    }

    public function testAnUnsupportedMessageFormatVersionIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        // The record batch v2 of Kafka 0.11 is not a message format of this protocol line
        $this->expectExceptionMessage('0.11.0');

        ProducerConfig::messageFormatMagic('0.11.0');
    }

    public function testAnUnsupportedMagicByteIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        ProducerConfig::messageFormatMagic(2);
    }
}
