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

namespace Protocol\Kafka\Tests\Unit\Consumer;

use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\InvalidGroupIdException;
use Protocol\Kafka\Consumer\AcknowledgeType;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\KafkaShareConsumer;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;

/**
 * What the share consumer of KIP-932 decides before it talks to a node: the options it refuses, the acknowledgement
 * modes and the state checks of the Java `ShareConsumerImpl` @ 4.3.1. The consumer below never connects anywhere -
 * the bootstrap address does not exist.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 */
#[CoversClass(KafkaShareConsumer::class)]
#[CoversClass(AcknowledgeType::class)]
final class KafkaShareConsumerTest extends TestCase
{
    /**
     * @return iterable<string, array{string, mixed}>
     */
    public static function unsupportedOptions(): iterable
    {
        foreach (ConsumerConfig::SHARE_GROUP_UNSUPPORTED_CONFIGS as $option) {
            yield $option => [$option, 'x'];
        }
    }

    #[DataProvider('unsupportedOptions')]
    public function testAnOptionOfAClassicOrKip848ConsumerIsRefused(string $option, mixed $value): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage("{$option} cannot be set when using a share group.");

        new KafkaShareConsumer([$option => $value] + self::configuration());
    }

    public function testTheRefusedOptionsAreTheOnesOfTheJavaShareConsumerConfig(): void
    {
        self::assertSame(
            [
                'auto.offset.reset', 'enable.auto.commit', 'group.instance.id', 'isolation.level',
                'partition.assignment.strategy', 'session.timeout.ms', 'heartbeat.interval.ms', 'group.protocol',
                'group.remote.assignor',
            ],
            ConsumerConfig::SHARE_GROUP_UNSUPPORTED_CONFIGS
        );
        self::assertSame(ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_IMPLICIT, ConsumerConfig::getDefaultConfiguration()[ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE]);
        self::assertSame(ConsumerConfig::SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED, ConsumerConfig::getDefaultConfiguration()[ConsumerConfig::SHARE_ACQUIRE_MODE]);
    }

    /**
     * @return iterable<string, array{string, mixed}>
     */
    public static function invalidModes(): iterable
    {
        yield 'acknowledgement mode' => [ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE, 'automatic'];
        yield 'acquire mode'         => [ConsumerConfig::SHARE_ACQUIRE_MODE, 'record-limit'];
        yield 'max.poll.records'     => [ConsumerConfig::MAX_POLL_RECORDS, 0];
    }

    #[DataProvider('invalidModes')]
    public function testAnUnknownValueOfAShareOptionIsRefused(string $option, mixed $value): void
    {
        $this->expectException(InvalidConfigurationException::class);

        new KafkaShareConsumer([$option => $value] + self::configuration());
    }

    public function testTheModesAreReadCaseInsensitively(): void
    {
        $consumer = new KafkaShareConsumer([
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'EXPLICIT',
            ConsumerConfig::SHARE_ACQUIRE_MODE         => 'Record_Limit',
        ] + self::configuration());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('The record cannot be acknowledged.');
        $consumer->acknowledge(new ConsumerRecord('orders', 0, 'v', null, 0, 7));
    }

    public function testAcknowledgeIsRefusedInTheImplicitMode(): void
    {
        $consumer = new KafkaShareConsumer(self::configuration());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Implicit acknowledgement of delivery is being used.');
        $consumer->acknowledge(new ConsumerRecord('orders', 0, 'v', null, 0, 7), AcknowledgeType::REJECT);
    }

    public function testAcknowledgeTakesOneOfTheTwoFormsOfTheJavaClient(): void
    {
        $consumer = new KafkaShareConsumer([ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => 'explicit'] + self::configuration());

        try {
            $consumer->acknowledge('orders', AcknowledgeType::ACCEPT);
            self::fail('a topic needs a partition, an offset and a type');
        } catch (InvalidArgumentException) {
        }

        $this->expectException(InvalidArgumentException::class);
        $consumer->acknowledge(new ConsumerRecord('orders', 0, 'v', null, 0, 7), 0, 7, AcknowledgeType::ACCEPT);
    }

    public function testAPollWithoutASubscriptionIsRefused(): void
    {
        $consumer = new KafkaShareConsumer(self::configuration());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Consumer is not subscribed to any topics.');
        $consumer->poll(0);
    }

    public function testANegativeTimeoutIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new KafkaShareConsumer(self::configuration())->poll(-1);
    }

    public function testASubscriptionNeedsAGroup(): void
    {
        $consumer = new KafkaShareConsumer([ConsumerConfig::GROUP_ID => ''] + self::configuration());

        $this->expectException(InvalidGroupIdException::class);
        $consumer->subscribe(['orders']);
    }

    public function testASubscriptionIsReplacedAndAnEmptyTopicNameIsRefused(): void
    {
        $consumer = new KafkaShareConsumer(self::configuration());
        $consumer->subscribe(['orders', 'payments', 'orders']);
        self::assertSame(['orders', 'payments'], $consumer->subscription());

        $this->expectException(InvalidArgumentException::class);
        $consumer->subscribe(['orders', ' ']);
    }

    public function testAClosedConsumerRefusesEveryCall(): void
    {
        $consumer = new KafkaShareConsumer(self::configuration());
        $consumer->close();
        $consumer->close();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('This consumer has already been closed.');
        $consumer->subscription();
    }

    public function testTheAcknowledgeTypesAreTheIdsOfTheWire(): void
    {
        self::assertSame(
            [ShareAcknowledgementBatch::ACCEPT, ShareAcknowledgementBatch::RELEASE, ShareAcknowledgementBatch::REJECT, ShareAcknowledgementBatch::RENEW],
            array_map(static fn(AcknowledgeType $type): int => $type->value, AcknowledgeType::cases())
        );
        self::assertSame(AcknowledgeType::RENEW, AcknowledgeType::forId(4));
        self::assertSame('release', AcknowledgeType::RELEASE->typeName());

        $this->expectException(InvalidArgumentException::class);
        AcknowledgeType::forId(ShareAcknowledgementBatch::GAP);
    }

    /**
     * @return array<string, mixed>
     */
    private static function configuration(): array
    {
        return [
            ConsumerConfig::BOOTSTRAP_SERVERS => ['tcp://share-consumer.invalid:9092'],
            ConsumerConfig::GROUP_ID          => 'share-group',
        ];
    }
}
