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

namespace Protocol\Kafka\Tests\Unit\Admin;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\Config;
use Protocol\Kafka\Admin\ConfigEntry;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\DeletedRecords;
use Protocol\Kafka\Admin\RecordsToDelete;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponse;

/**
 * Tests the value objects of the admin apis Kafka 0.11 added.
 *
 * @see docs/protocol/0.11.0.md, sections "DeleteRecords API (key 21, v0)" and "DescribeConfigs API (key 32, v0)"
 */
#[CoversClass(ConfigResource::class)]
#[CoversClass(ConfigEntry::class)]
#[CoversClass(Config::class)]
#[CoversClass(RecordsToDelete::class)]
#[CoversClass(DeletedRecords::class)]
final class ConfigResourceTest extends TestCase
{
    /**
     * A DescribeConfigs answer with one topic resource that has one configured and one default option
     */
    private const string RESPONSE_HEX = '00000051'
        . '00000001'
        . '00000000'
        . '00000001'
        . '0000' . 'ffff' . '02' . '0005' . '746f706963'
        . '00000002'
        . '000c' . '726574656e74696f6e2e6d73' . '0007' . '33363030303030' . '00' . '00' . '00'
        . '000e' . '636c65616e75702e706f6c696379' . '0006' . '64656c657465' . '00' . '01' . '00';

    public function testTheResourceTypesAreTheOnesOfTheZeroElevenSources(): void
    {
        // `org.apache.kafka.common.requests.ResourceType` @ 0.11.0.3, NOT the ConfigResource.Type of Kafka 1.0
        self::assertSame(0, ConfigResource::TYPE_UNKNOWN);
        self::assertSame(1, ConfigResource::TYPE_ANY);
        self::assertSame(2, ConfigResource::TYPE_TOPIC);
        self::assertSame(3, ConfigResource::TYPE_GROUP);
        self::assertSame(4, ConfigResource::TYPE_BROKER);
    }

    public function testAResourceIsAddressedByItsTypeAndItsName(): void
    {
        $topic  = ConfigResource::topic('0');
        $broker = ConfigResource::broker(0);

        self::assertSame('topic:0', $topic->key());
        self::assertSame('broker:0', $broker->key());
        self::assertNotSame($topic->key(), $broker->key(), 'the topic 0 is not the broker 0');
        self::assertSame('broker:0', (string) $broker);
    }

    public function testABrokerIdTravelsAsText(): void
    {
        self::assertSame('7', ConfigResource::broker(7)->name);
        self::assertSame('7', ConfigResource::broker('7')->name);
    }

    public function testAKeyIsParsedBackIntoTheResource(): void
    {
        $resource = ConfigResource::fromKey('topic:my:topic');

        self::assertSame(ConfigResource::TYPE_TOPIC, $resource->type);
        self::assertSame('my:topic', $resource->name, 'only the first colon separates the type from the name');
    }

    public function testAnUnknownKeyIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConfigResource::fromKey('cluster:0');
    }

    public function testAKeyWithoutATypeIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConfigResource::fromKey('topic');
    }

    public function testAResourceTypeThisClientDoesNotKnowBecomesUnknown(): void
    {
        $resource = ConfigResource::fromWire(9, 'whatever');

        self::assertSame(ConfigResource::TYPE_UNKNOWN, $resource->type);
        self::assertSame('unknown:whatever', $resource->key());
    }

    public function testAConfigIsBuiltFromTheAnswerOfTheBroker(): void
    {
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        $config = Config::fromResponseResource($response->resources[0]);

        self::assertSame('topic:topic', $config->resource->key());
        self::assertSame(['retention.ms', 'cleanup.policy'], array_keys($config->entries));
        self::assertSame('3600000', $config->value('retention.ms'));
        self::assertFalse($config->get('retention.ms')->isDefault);
        self::assertTrue($config->get('cleanup.policy')->isDefault);
        self::assertNull($config->get('no.such.option'), 'an option the resource does not have is null');
        self::assertNull($config->value('no.such.option'));
    }

    public function testTheNonDefaultValuesAreWhatAnAlterConfigsHasToSendBack(): void
    {
        // AlterConfigs replaces the whole configuration, so keeping a topic as it is means sending exactly the
        // options that are not defaults - see the "AlterConfigs API (key 33, v0)" section
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        $config = Config::fromResponseResource($response->resources[0]);

        self::assertSame(['retention.ms' => '3600000'], $config->nonDefaultValues());
    }

    public function testRecordsToDeleteNamesTheFirstOffsetThatSurvives(): void
    {
        self::assertSame(100, RecordsToDelete::beforeOffset(100)->beforeOffset);
        self::assertSame(
            DeleteRecordsRequest::HIGH_WATERMARK,
            RecordsToDelete::allRecords()->beforeOffset,
            'everything up to the high watermark is the offset -1 of the wire'
        );
        self::assertSame(RecordsToDelete::HIGH_WATERMARK, DeleteRecordsRequest::HIGH_WATERMARK);
    }

    public function testDeletedRecordsCarriesTheNewLowWatermark(): void
    {
        self::assertSame(42, new DeletedRecords(42)->lowWatermark);
    }
}
