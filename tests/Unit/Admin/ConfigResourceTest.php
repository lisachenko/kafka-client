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
use Protocol\Kafka\Admin\ConfigSource;
use Protocol\Kafka\Admin\ConfigSynonym;
use Protocol\Kafka\Admin\DeletedRecords;
use Protocol\Kafka\Admin\RecordsToDelete;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponse;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponseV0;

/**
 * Tests the value objects of the admin apis Kafka 0.11 added.
 *
 * @see docs/protocol/2.8.md, sections "DeleteRecords API (key 21, v0)" and "DescribeConfigs API (key 32, v0 and v1)"
 */
#[CoversClass(ConfigResource::class)]
#[CoversClass(ConfigEntry::class)]
#[CoversClass(ConfigSource::class)]
#[CoversClass(ConfigSynonym::class)]
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

    /**
     * The same topic in a version 1 answer, with the config sources and the synonyms of KIP-226
     *
     *   retention.ms   => "3600000",    ConfigSource => 01 (the topic set it), one synonym of its own name
     *   segment.bytes  => "1073741824", ConfigSource => 04 (the server.properties of the broker set the synonym
     *                     `log.segment.bytes`), two synonyms: the static one and the built-in default behind it
     *   cleanup.policy => "delete",     ConfigSource => 05 (nobody set it), no synonym at all
     */
    private const string RESPONSE_V1_HEX = '000000d30000000100000000000000010000ffff020005746f70696300000003000c726574656e74696f6e2e6d7300073336303030303000010000000001000c726574656e74696f6e2e6d7300073336303030303001000d7365676d656e742e6279746573000a313037333734313832340004000000000200116c6f672e7365676d656e742e6279746573000a313037333734313832340400116c6f672e7365676d656e742e6279746573000a3130373337343138323405000e636c65616e75702e706f6c696379000664656c65746500050000000000';

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
        $response = DescribeConfigsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

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
        $response = DescribeConfigsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        $config = Config::fromResponseResource($response->resources[0]);

        self::assertSame(['retention.ms' => '3600000'], $config->nonDefaultValues());
        self::assertSame(
            ['retention.ms' => '3600000'],
            $config->ownValues(),
            'the source of a version 0 entry is derived from its is_default flag and the resource type'
        );
    }

    public function testTheSourcesAreTheOnesOfTheWireEnum(): void
    {
        // `DescribeConfigsResponse.ConfigSource` @ 1.1.1, whose ids are ordered by precedence
        self::assertSame(0, ConfigSource::UNKNOWN);
        self::assertSame(1, ConfigSource::TOPIC_CONFIG);
        self::assertSame(2, ConfigSource::DYNAMIC_BROKER_CONFIG);
        self::assertSame(3, ConfigSource::DYNAMIC_DEFAULT_BROKER_CONFIG);
        self::assertSame(4, ConfigSource::STATIC_BROKER_CONFIG);
        self::assertSame(5, ConfigSource::DEFAULT_CONFIG);

        // The names are the ones of the Java admin client, whose enum calls the first two differently
        self::assertSame('DYNAMIC_TOPIC_CONFIG', ConfigSource::nameOf(ConfigSource::TOPIC_CONFIG));
        self::assertSame('STATIC_BROKER_CONFIG', ConfigSource::nameOf(ConfigSource::STATIC_BROKER_CONFIG));
        self::assertSame('UNKNOWN', ConfigSource::nameOf(9), 'a source this client does not know has no name');
        self::assertSame(ConfigSource::UNKNOWN, ConfigSource::fromWire(9), 'and folds into UNKNOWN');
        self::assertSame(ConfigSource::DEFAULT_CONFIG, ConfigSource::fromWire(5));
    }

    public function testOnlyTheSourceOfTheResourceItselfCountsAsItsOwn(): void
    {
        self::assertTrue(ConfigSource::isResourceOwn(ConfigSource::TOPIC_CONFIG));
        self::assertTrue(ConfigSource::isResourceOwn(ConfigSource::DYNAMIC_BROKER_CONFIG));
        self::assertFalse(
            ConfigSource::isResourceOwn(ConfigSource::DYNAMIC_DEFAULT_BROKER_CONFIG),
            'a cluster-wide default belongs to the cluster, not to the broker that reports it'
        );
        self::assertFalse(ConfigSource::isResourceOwn(ConfigSource::STATIC_BROKER_CONFIG));
        self::assertFalse(ConfigSource::isResourceOwn(ConfigSource::DEFAULT_CONFIG));
        self::assertFalse(ConfigSource::isResourceOwn(ConfigSource::UNKNOWN));
    }

    public function testAVersionOneAnswerCarriesTheSourceAndTheSynonymsOfEveryEntry(): void
    {
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        $config = Config::fromResponseResource($response->resources[0]);

        self::assertSame(ConfigSource::TOPIC_CONFIG, $config->get('retention.ms')->source);
        self::assertSame(ConfigSource::STATIC_BROKER_CONFIG, $config->get('segment.bytes')->source);
        self::assertSame(ConfigSource::DEFAULT_CONFIG, $config->get('cleanup.policy')->source);

        // `isDefault` is derived from the source, as `ConfigEntry.isDefault()` @ 1.1.1 does it
        self::assertFalse($config->get('retention.ms')->isDefault);
        self::assertFalse(
            $config->get('segment.bytes')->isDefault,
            'an option whose broker synonym is configured is not a default, although the topic did not set it'
        );
        self::assertTrue($config->get('cleanup.policy')->isDefault);

        $synonyms = $config->get('segment.bytes')->synonyms;
        self::assertCount(2, $synonyms);
        self::assertContainsOnlyInstancesOf(ConfigSynonym::class, $synonyms);
        self::assertSame('log.segment.bytes', $synonyms[0]->name, 'the winning value comes first');
        self::assertSame('1073741824', $synonyms[0]->value);
        self::assertSame(ConfigSource::STATIC_BROKER_CONFIG, $synonyms[0]->source);
        self::assertSame(ConfigSource::DEFAULT_CONFIG, $synonyms[1]->source, 'the value it shadows');
        self::assertSame([], $config->get('cleanup.policy')->synonyms);
    }

    public function testOnlyTheOptionsOfTheResourceItselfAreSentBackByAnAlterConfigs(): void
    {
        // `nonDefaultValues()` reports what the broker does not call a default, which since KIP-226 includes the
        // options its own server.properties sets; `ownValues()` is the set the resource really carries
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        $config = Config::fromResponseResource($response->resources[0]);

        self::assertSame(
            ['retention.ms' => '3600000', 'segment.bytes' => '1073741824'],
            $config->nonDefaultValues()
        );
        self::assertSame(['retention.ms' => '3600000'], $config->ownValues());
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
