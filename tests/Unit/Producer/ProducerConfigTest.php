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
use Protocol\Kafka\Common\Record\RecordBatch;
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

    public function testTheTransactionalDeliveryOfKafka011IsOffByDefault(): void
    {
        // `transactional.id` arrived with Kafka 0.11 (KIP-98) and the option exists on this line, but a producer
        // that does not name one is not transactional at all
        $configuration = ProducerConfig::getDefaultConfiguration();

        self::assertArrayHasKey(ProducerConfig::TRANSACTIONAL_ID, $configuration);
        self::assertNull($configuration[ProducerConfig::TRANSACTIONAL_ID]);
        self::assertFalse($configuration[ProducerConfig::ENABLE_IDEMPOTENCE]);
        self::assertSame(60000, $configuration[ProducerConfig::TRANSACTION_TIMEOUT_MS]);
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
        $this->expectExceptionMessage('none, gzip, snappy, lz4, zstd');

        ProducerConfig::compressionCodec('brotli');
    }

    public function testTheZstdCompressionTypeIsRefusedWithoutTheExtension(): void
    {
        // zstd (Kafka 2.1, KIP-110) is the one codec this package can not implement itself: `ext-zstd` or nothing
        if (CompressionCodec::isZstdAvailable()) {
            self::assertSame(CompressionCodec::ZSTD, ProducerConfig::compressionCodec('zstd'));

            return;
        }

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('needs the ext-zstd extension');

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

    public function testTheProducerWritesTheRecordBatchOfMessageFormatV2ByDefault(): void
    {
        $configuration = ProducerConfig::getDefaultConfiguration();

        self::assertSame(
            ProducerConfig::MESSAGE_FORMAT_VERSION_0_11_0,
            $configuration[ProducerConfig::MESSAGE_FORMAT_VERSION]
        );
        self::assertSame(
            RecordBatch::MAGIC,
            ProducerConfig::messageFormatMagic($configuration[ProducerConfig::MESSAGE_FORMAT_VERSION])
        );
        self::assertSame(
            Message::MAGIC_V1,
            ProducerConfig::messageFormatMagic(ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0),
            'the message format of Kafka 0.10 stays available for a topic that is configured for it'
        );
    }

    public function testAnUnsupportedMessageFormatVersionIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        // Kafka 1.0 kept the message format v2, but this client only names the releases it knows
        $this->expectExceptionMessage('1.0.0');

        ProducerConfig::messageFormatMagic('1.0.0');
    }

    public function testAnUnsupportedMagicByteIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        ProducerConfig::messageFormatMagic(3);
    }

    public function testIdempotenceIsOffByDefault(): void
    {
        $configuration = ProducerConfig::getDefaultConfiguration();

        self::assertFalse($configuration[ProducerConfig::ENABLE_IDEMPOTENCE]);
        self::assertSame(60000, $configuration[ProducerConfig::TRANSACTION_TIMEOUT_MS]);
        self::assertSame([], ProducerConfig::resolveIdempotence([]), 'nothing is overridden without the option');
    }

    #[DataProvider('idempotenceValues')]
    public function testTheOptionIsReadAsABooleanOrAsItsStringSpelling(mixed $value, bool $enabled): void
    {
        self::assertSame($enabled, ProducerConfig::isIdempotenceEnabled($value));
    }

    /**
     * @return iterable<string, array{0: mixed, 1: bool}>
     */
    public static function idempotenceValues(): iterable
    {
        yield 'boolean true'  => [true, true];
        yield 'boolean false' => [false, false];
        yield 'string true'   => ['true', true];
        yield 'string TRUE'   => ['TRUE', true];
        yield 'string false'  => ['false', false];
        yield 'the number 1'  => [1, true];
        yield 'the number 0'  => [0, false];
    }

    public function testIdempotenceOverridesTheAcksAndTheRetriesTheCallerLeftAlone(): void
    {
        $configuration = ProducerConfig::resolveIdempotence([ProducerConfig::ENABLE_IDEMPOTENCE => true]);

        self::assertSame(ProducerConfig::ACKS_ALL, $configuration[ProducerConfig::ACKS]);
        self::assertSame(
            ProducerConfig::DEFAULT_IDEMPOTENT_RETRIES,
            $configuration[ProducerConfig::RETRIES],
            'the Java producer uses Integer.MAX_VALUE here, a synchronous client can not'
        );
    }

    public function testAnExplicitAcksOfAllIsAcceptedAndNormalizedIntoTheValueOfTheWire(): void
    {
        $configuration = ProducerConfig::resolveIdempotence([
            ProducerConfig::ENABLE_IDEMPOTENCE => true,
            ProducerConfig::ACKS               => 'all',
            ProducerConfig::RETRIES            => 7,
        ]);

        self::assertSame(ProducerConfig::ACKS_ALL, $configuration[ProducerConfig::ACKS]);
        self::assertSame(7, $configuration[ProducerConfig::RETRIES], 'an explicit retry budget is kept');
    }

    #[DataProvider('acksThatIdempotenceCanNotLiveWith')]
    public function testAnAcksBelowAllIsRefusedNextToIdempotence(mixed $acks): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Must set acks to all in order to use the idempotent producer');

        ProducerConfig::resolveIdempotence([
            ProducerConfig::ENABLE_IDEMPOTENCE => true,
            ProducerConfig::ACKS               => $acks,
        ]);
    }

    /**
     * @return iterable<string, array{0: mixed}>
     */
    public static function acksThatIdempotenceCanNotLiveWith(): iterable
    {
        yield 'fire and forget'      => [ProducerConfig::ACKS_NONE];
        yield 'the leader alone'     => [ProducerConfig::ACKS_LEADER];
        yield 'the leader as string' => ['1'];
    }

    public function testARetryBudgetOfZeroIsRefusedNextToIdempotence(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Must set retries to non-zero when using the idempotent producer');

        ProducerConfig::resolveIdempotence([
            ProducerConfig::ENABLE_IDEMPOTENCE => true,
            ProducerConfig::RETRIES            => 0,
        ]);
    }

    public function testAConfigurationWithoutIdempotenceIsNotTouchedAtAll(): void
    {
        $original = [ProducerConfig::ACKS => ProducerConfig::ACKS_NONE, ProducerConfig::RETRIES => 0];

        self::assertSame($original, ProducerConfig::resolveIdempotence($original));
    }

    public function testTheStringAllIsTheAcksValueOfTheWire(): void
    {
        self::assertSame(ProducerConfig::ACKS_ALL, ProducerConfig::parseAcks('all'));
        self::assertSame(ProducerConfig::ACKS_ALL, ProducerConfig::parseAcks(' ALL '));
        self::assertSame(ProducerConfig::ACKS_ALL, ProducerConfig::parseAcks('-1'));
        self::assertSame(ProducerConfig::ACKS_LEADER, ProducerConfig::parseAcks('1'));
        self::assertSame(ProducerConfig::ACKS_NONE, ProducerConfig::parseAcks(0));
    }
}
