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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\ConfigSource;
use Protocol\Kafka\Admin\ConfigType;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntry;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntryV0;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntryV1;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigSynonym;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResourceV0;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequest;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequestV2;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponse;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponseV1;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponseV2;

/**
 * Byte-exact tests for the DescribeConfigs API (api key 32), version 0 of Kafka 0.11 and version 1 of Kafka 1.1.
 *
 * KIP-226 changed the config ENTRY of the answer and nothing else: the `is_default` boolean became a
 * `config_source` int8 in the same place and the entry gained its synonyms, while the request gained the trailing
 * `include_synonyms` boolean. Kafka 2.0 raised the api to **version 2** without touching a byte
 * (`DESCRIBE_CONFIGS_REQUEST_V2 = DESCRIBE_CONFIGS_REQUEST_V1` @ 2.0.1, KIP-219), which is the version the client
 * sends. All three are exercised here, and the derivation of the source from the boolean of a version 0 answer.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v3)"
 */
#[CoversClass(DescribeConfigsRequest::class)]
#[CoversClass(DescribeConfigsRequestV1::class)]
#[CoversClass(DescribeConfigsRequestV0::class)]
#[CoversClass(DescribeConfigsResponse::class)]
#[CoversClass(ConfigType::class)]
#[CoversClass(DescribeConfigsRequestV2::class)]
#[CoversClass(DescribeConfigsResponseV1::class)]
#[CoversClass(DescribeConfigsResponseV2::class)]
#[CoversClass(DescribeConfigsResponseConfigEntryV1::class)]
#[CoversClass(DescribeConfigsResponseV0::class)]
#[CoversClass(DescribeConfigsRequestResource::class)]
#[CoversClass(DescribeConfigsResponseResource::class)]
#[CoversClass(DescribeConfigsResponseResourceV0::class)]
#[CoversClass(DescribeConfigsResponseConfigEntry::class)]
#[CoversClass(DescribeConfigsResponseConfigEntryV0::class)]
#[CoversClass(DescribeConfigsResponseConfigSynonym::class)]
final class DescribeConfigsTest extends TestCase
{
    /**
     * DescribeConfigs request v0 for one named option of a topic and every option of a broker.
     *
     *   Size          => 00 00 00 34 (52 bytes)
     *   ApiKey        => 00 20 (32)
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 07
     *   ClientId      => 00 04 "test"
     *   Resources     => 00 00 00 02
     *     ResourceType => 02 (topic), ResourceName => 00 05 "topic"
     *       ConfigNames => 00 00 00 01, 00 0c "retention.ms"
     *     ResourceType => 04 (broker), ResourceName => 00 01 "0"
     *       ConfigNames => ff ff ff ff (null: every option)
     */
    private const string REQUEST_HEX = '00000034'
        . '0020'
        . '0000'
        . '00000007'
        . '0004' . '74657374'
        . '00000002'
        . '02' . '0005' . '746f706963' . '00000001' . '000c' . '726574656e74696f6e2e6d73'
        . '04' . '0001' . '30' . 'ffffffff';

    /**
     * The very same request as version 1: the same resources, plus the trailing `include_synonyms = 01`.
     *
     *   Size          => 00 00 00 35 (53 bytes, one more than version 0)
     *   ApiVersion    => 00 01
     *   …             => the two resources of REQUEST_HEX, byte for byte
     *   IncludeSynonyms => 01
     */
    private const string REQUEST_V1_HEX = '00000035'
        . '0020'
        . '0001'
        . '00000007'
        . '0004' . '74657374'
        . '00000002'
        . '02' . '0005' . '746f706963' . '00000001' . '000c' . '726574656e74696f6e2e6d73'
        . '04' . '0001' . '30' . 'ffffffff'
        . '01';

    /**
     * DescribeConfigs response v0: one topic entry and one resource that the broker refused.
     *
     *   Size           => 00 00 00 3f (63 bytes)
     *   CorrelationId  => 00 00 00 07
     *   ThrottleTimeMs => 00 00 00 00
     *   Resources      => 00 00 00 02
     *     ErrorCode => 00 00, ErrorMessage => ff ff, Type => 02, Name => 00 05 "topic"
     *       ConfigEntries => 00 00 00 01
     *         00 0c "retention.ms", 00 01 "1", ReadOnly => 00, IsDefault => 01, IsSensitive => 00
     *     ErrorCode => 00 2a, ErrorMessage => 00 03 "bad", Type => 04, Name => 00 01 "7"
     *       ConfigEntries => 00 00 00 00
     */
    private const string RESPONSE_HEX = '0000003f'
        . '00000007'
        . '00000000'
        . '00000002'
        . '0000' . 'ffff' . '02' . '0005' . '746f706963'
        . '00000001' . '000c' . '726574656e74696f6e2e6d73' . '0001' . '31' . '00' . '01' . '00'
        . '002a' . '0003' . '626164' . '04' . '0001' . '37'
        . '00000000';

    /**
     * DescribeConfigs response v1 of one topic whose option was set on the topic itself.
     *
     *   Size           => 00 00 00 64 (100 bytes)
     *   CorrelationId  => 00 00 00 07
     *   ThrottleTimeMs => 00 00 00 00
     *   Resources      => 00 00 00 01
     *     ErrorCode => 00 00, ErrorMessage => ff ff, Type => 02, Name => 00 05 "topic"
     *       ConfigEntries => 00 00 00 01
     *         00 0c "retention.ms", 00 01 "1", ReadOnly => 00, ConfigSource => 01, IsSensitive => 00
     *         ConfigSynonyms => 00 00 00 02
     *           00 0c "retention.ms",     00 01 "1",         ConfigSource => 01 (the topic's own value, which won)
     *           00 10 "log.retention.ms", 00 09 "604800000", ConfigSource => 05 (the built-in default it shadows)
     */
    private const string RESPONSE_V1_HEX = '00000064'
        . '00000007'
        . '00000000'
        . '00000001'
        . '0000' . 'ffff' . '02' . '0005' . '746f706963'
        . '00000001'
        . '000c' . '726574656e74696f6e2e6d73' . '0001' . '31' . '00' . '01' . '00'
        . '00000002'
        . '000c' . '726574656e74696f6e2e6d73' . '0001' . '31' . '01'
        . '0010' . '6c6f672e726574656e74696f6e2e6d73' . '0009' . '363034383030303030' . '05';

    /**
     * The entry of a sensitive option: `is_sensitive = 01` and a value of `ff ff`, the null of a NULLABLE_STRING
     */
    private const string SENSITIVE_RESPONSE_HEX = '00000034'
        . '00000007'
        . '00000000'
        . '00000001'
        . '0000' . 'ffff' . '04' . '0001' . '30'
        . '00000001' . '0015' . '73736c2e6b657973746f72652e70617373776f7264' . 'ffff' . '01' . '00' . '01';

    /**
     * The same sensitive option in a version 1 answer: the value is null in the entry AND in its synonym.
     *
     *     00 10 "ssl.key.password", ff ff, ReadOnly => 00, ConfigSource => 04, IsSensitive => 01
     *     ConfigSynonyms => 00 00 00 01
     *       00 10 "ssl.key.password", ff ff, ConfigSource => 04
     */
    private const string SENSITIVE_RESPONSE_V1_HEX = '00000048'
        . '00000007'
        . '00000000'
        . '00000001'
        . '0000' . 'ffff' . '04' . '0001' . '30'
        . '00000001'
        . '0010' . '73736c2e6b65792e70617373776f7264' . 'ffff' . '00' . '04' . '01'
        . '00000001'
        . '0010' . '73736c2e6b65792e70617373776f7264' . 'ffff' . '04';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DescribeConfigsRequestV0(
            [
                new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', ['retention.ms']),
                DescribeConfigsRequestResource::fromConfigResource(ConfigResource::broker(0)),
            ],
            'test',
            7
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_CONFIGS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'the version of a 0.11.0.3 broker has no include_synonyms');
    }

    public function testTheVersionOneRequestIsTheVersionZeroFrameWithTheSynonymFlag(): void
    {
        $resources = [
            new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', ['retention.ms']),
            DescribeConfigsRequestResource::fromConfigResource(ConfigResource::broker(0)),
        ];

        $request = new DescribeConfigsRequestV1($resources, true, false, 'test', 7);

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion(), 'the version Kafka 1.1 added with KIP-226');
        self::assertSame(
            strlen((string) new DescribeConfigsRequestV0($resources, 'test', 7)) + 1,
            strlen((string) $request),
            'version 1 is version 0 plus one byte'
        );
        self::assertStringEndsWith(
            '00',
            bin2hex((string) new DescribeConfigsRequestV1($resources, false, false, 'test', 7)),
            'and the flag is the last byte of the frame'
        );
    }

    public function testTheClientSendsTheVersionTwoOfKafkaTwoZero(): void
    {
        $resources = [
            new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', ['retention.ms']),
            DescribeConfigsRequestResource::fromConfigResource(ConfigResource::broker(0)),
        ];

        $request = new DescribeConfigsRequestV2($resources, true, false, 'test', 7);

        // `DESCRIBE_CONFIGS_REQUEST_V2 = DESCRIBE_CONFIGS_REQUEST_V1` @ 2.0.1: only the version field is different
        self::assertSame(2, $request->getApiVersion());
        self::assertSame(substr_replace(self::REQUEST_V1_HEX, '0002', 12, 4), bin2hex((string) $request));

        $answer     = DescribeConfigsResponseV2::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));
        $versionOne = DescribeConfigsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertSame(
            bin2hex((string) $versionOne),
            bin2hex((string) $answer),
            'the answer of version 2 has the layout of version 1'
        );
        self::assertSame(self::RESPONSE_V1_HEX, bin2hex((string) $answer));
    }

    /**
     * DescribeConfigs request v3 for two named options of a topic, with the synonyms and without the documentation.
     *
     *   ApiVersion            => 00 03
     *   Resources             => 00 00 00 01, 02, 00 09 "t4-26-own", 00 00 00 02, "segment.bytes", "retention.ms"
     *   IncludeSynonyms       => 01
     *   IncludeDocumentation  => 00
     */
    private const string REQUEST_V3_HEX = '000000470020000300000516000a74342d766563746f72730000000102000974342d32362d6f776e00000002000d7365676d'
        . '656e742e6279746573000c726574656e74696f6e2e6d730100';

    /**
     * The answer of that request: every entry ends in the `config_type` byte and the null documentation of KIP-569
     */
    private const string RESPONSE_V3_HEX = '000000c20000051600000000000000010000000002000974342d32362d6f776e00000002000d7365676d656e742e6279746573000931303438353736303000010000000003000d7365676d656e742e627974657300093130343835373630300100'
        . '116c6f672e7365676d656e742e6279746573000a313037333734313832340400116c6f672e7365676d656e742e6279746573000a313037333734313832340503ffff000c726574656e74696f6e2e6d7300093630343830303030300005000000000005ffff';

    /**
     * The version Kafka 2.6 added: a second boolean in the request, a type and a documentation in every entry
     */
    public function testTheClientSendsTheVersionThreeOfKafkaTwoSix(): void
    {
        $request = new DescribeConfigsRequest(
            [new DescribeConfigsRequestResource(
                ConfigResource::TYPE_TOPIC,
                't4-26-own',
                ['segment.bytes', 'retention.ms']
            )],
            true,
            false,
            't4-vectors',
            1302
        );

        self::assertSame(3, $request->getApiVersion());
        self::assertSame(self::REQUEST_V3_HEX, bin2hex((string) $request));
        self::assertSame(
            [
                'messageSize',
                'apiKey',
                'apiVersion',
                'correlationId',
                'clientId',
                'resources',
                'includeSynonyms',
                'includeDocumentation',
            ],
            array_keys(DescribeConfigsRequest::getScheme()),
            'the flag of KIP-569 is the last field of the frame, behind the one of KIP-226'
        );
    }

    public function testTheEntryOfVersionThreeEndsInTheTypeAndTheDocumentation(): void
    {
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V3_HEX)));
        $entries  = $response->resources[0]->configEntries;

        self::assertSame(ConfigType::INT, $entries['segment.bytes']->configType, 'segment.bytes is an INT');
        self::assertSame(ConfigType::LONG, $entries['retention.ms']->configType, 'retention.ms is a LONG');
        self::assertNull($entries['segment.bytes']->documentation, 'the request did not ask for it');
        self::assertNull($entries['retention.ms']->documentation);
        self::assertCount(3, $entries['segment.bytes']->configSynonyms, 'the synonyms of KIP-226 are still there');
        self::assertSame(
            [
                'configName',
                'configValue',
                'readOnly',
                'configSource',
                'isSensitive',
                'configSynonyms',
                'configType',
                'documentation',
            ],
            array_keys(DescribeConfigsResponseConfigEntry::getScheme()),
            'both fields stand BEHIND the synonyms'
        );
        self::assertSame(self::RESPONSE_V3_HEX, bin2hex((string) $response));
    }

    /**
     * An answer of a lower version carries neither field, and the client reads the defaults of the two
     */
    public function testAnEntryOfVersionOneHasNoTypeAndNoDocumentation(): void
    {
        $response = DescribeConfigsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));
        $entry    = $response->resources[0]->configEntries['retention.ms'];

        self::assertSame(ConfigType::UNKNOWN, $entry->configType, 'the `"default": "0"` of the field');
        self::assertNull($entry->documentation);
        self::assertSame(
            ['configName', 'configValue', 'readOnly', 'configSource', 'isSensitive', 'configSynonyms'],
            array_keys(DescribeConfigsResponseConfigEntryV1::getScheme())
        );
    }

    /**
     * The ids of the type byte are the ordinals of `DescribeConfigsResponse.ConfigType` @ 2.8.2
     */
    public function testTheConfigTypesAreTheOnesOfTheJavaClient(): void
    {
        self::assertSame(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [
                ConfigType::UNKNOWN,
                ConfigType::BOOLEAN,
                ConfigType::STRING,
                ConfigType::INT,
                ConfigType::SHORT,
                ConfigType::LONG,
                ConfigType::DOUBLE,
                ConfigType::LIST,
                ConfigType::CLASS_NAME,
                ConfigType::PASSWORD,
            ]
        );
        self::assertSame('CLASS', ConfigType::nameOf(ConfigType::CLASS_NAME), 'the name of the Java enum member');
        self::assertSame('PASSWORD', ConfigType::nameOf(ConfigType::PASSWORD));
        self::assertSame(ConfigType::UNKNOWN, ConfigType::fromWire(42), 'an id this client does not know');
        self::assertSame('UNKNOWN', ConfigType::nameOf(42));
    }

    public function testANullConfigNameArrayIsTheCountMinusOne(): void
    {
        $all = new DescribeConfigsRequest(
            [new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', null)],
            false,
            false,
            'test',
            7
        );
        $none = new DescribeConfigsRequest(
            [new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', [])],
            false,
            false,
            'test',
            7
        );

        // "every option" and "no option at all" are the same four bytes apart: ff ff ff ff against 00 00 00 00
        // (each followed by the two flags of the version 3 request, both false)
        self::assertStringEndsWith('ffffffff' . '0000', bin2hex((string) $all));
        self::assertStringEndsWith('00000000' . '0000', bin2hex((string) $none));
        self::assertSame(strlen((string) $all), strlen((string) $none));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DescribeConfigsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertCount(2, $response->resources, 'the resources are a list, not a map: type and name identify one');

        [$topic, $broker] = $response->resources;
        self::assertSame(KafkaException::NO_ERROR, $topic->errorCode);
        self::assertNull($topic->errorMessage);
        self::assertSame(ConfigResource::TYPE_TOPIC, $topic->resourceType);
        self::assertSame('topic', $topic->resourceName);
        self::assertSame(['retention.ms'], array_keys($topic->configEntries), 'entries are keyed by the option name');
        self::assertSame('1', $topic->configEntries['retention.ms']->configValue);
        self::assertFalse($topic->configEntries['retention.ms']->readOnly);
        self::assertTrue($topic->configEntries['retention.ms']->isDefault);
        self::assertFalse($topic->configEntries['retention.ms']->isSensitive);
        self::assertSame([], $topic->configEntries['retention.ms']->configSynonyms, 'version 0 has no synonyms');

        self::assertSame(KafkaException::INVALID_REQUEST, $broker->errorCode);
        self::assertSame('bad', $broker->errorMessage);
        self::assertSame(ConfigResource::TYPE_BROKER, $broker->resourceType);
        self::assertSame([], $broker->configEntries, 'a refused resource carries no entry at all');
    }

    public function testTheSourceOfAVersionZeroEntryIsDerivedFromTheDefaultFlag(): void
    {
        $response = DescribeConfigsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        $entry = $response->resources[0]->configEntries['retention.ms'];

        // `DescribeConfigsResponse(Struct)` @ 1.1.1 turns the boolean back into a source with the resource type
        self::assertSame(ConfigSource::DEFAULT_CONFIG, $entry->source(ConfigResource::TYPE_TOPIC));

        $configured            = new DescribeConfigsResponseConfigEntryV0();
        $configured->isDefault = false;
        self::assertSame(ConfigSource::TOPIC_CONFIG, $configured->source(ConfigResource::TYPE_TOPIC));
        self::assertSame(ConfigSource::STATIC_BROKER_CONFIG, $configured->source(ConfigResource::TYPE_BROKER));
        self::assertSame(ConfigSource::UNKNOWN, $configured->source(ConfigResource::TYPE_GROUP));
    }

    public function testTheVersionOneAnswerCarriesTheSourceAndTheSynonyms(): void
    {
        $response = DescribeConfigsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertCount(1, $response->resources);
        $entry = $response->resources[0]->configEntries['retention.ms'];

        self::assertSame('1', $entry->configValue);
        self::assertFalse($entry->readOnly);
        self::assertFalse($entry->isSensitive);
        self::assertSame(ConfigSource::TOPIC_CONFIG, $entry->configSource);
        self::assertSame(
            ConfigSource::TOPIC_CONFIG,
            $entry->source(ConfigResource::TYPE_TOPIC),
            'a version 1 entry reports the source the broker sent, whatever the resource type is'
        );

        self::assertCount(2, $entry->configSynonyms, 'the winning value first, the shadowed ones behind it');
        [$own, $shadowed] = $entry->configSynonyms;
        self::assertSame('retention.ms', $own->configName);
        self::assertSame('1', $own->configValue);
        self::assertSame(ConfigSource::TOPIC_CONFIG, $own->configSource);
        self::assertSame('log.retention.ms', $shadowed->configName, 'a synonym is not named like the option');
        self::assertSame('604800000', $shadowed->configValue);
        self::assertSame(ConfigSource::DEFAULT_CONFIG, $shadowed->configSource);
    }

    public function testTheValueOfASensitiveOptionIsNull(): void
    {
        $response = DescribeConfigsResponseV0::unpack(
            new StringStream((string) hex2bin(self::SENSITIVE_RESPONSE_HEX))
        );

        $entry = $response->resources[0]->configEntries['ssl.keystore.password'];

        self::assertTrue($entry->isSensitive);
        self::assertNull($entry->configValue, 'the broker never sends the value of a PASSWORD option');
        self::assertTrue($entry->readOnly, 'every option of a broker resource was read only on Kafka 0.11');
    }

    public function testTheValueOfASensitiveOptionIsNullInItsSynonymsAsWell(): void
    {
        $response = DescribeConfigsResponseV1::unpack(
            new StringStream((string) hex2bin(self::SENSITIVE_RESPONSE_V1_HEX))
        );

        $entry = $response->resources[0]->configEntries['ssl.key.password'];

        self::assertTrue($entry->isSensitive);
        self::assertNull($entry->configValue);
        self::assertSame(ConfigSource::STATIC_BROKER_CONFIG, $entry->configSource);
        self::assertFalse($entry->readOnly, 'an SSL keystore option is dynamically updatable since KIP-226');
        self::assertCount(1, $entry->configSynonyms);
        self::assertNull($entry->configSynonyms[0]->configValue, 'a synonym of a secret carries no value either');
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $version0 = DescribeConfigsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));
        $version1 = DescribeConfigsResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $version0));
        self::assertSame(self::RESPONSE_V1_HEX, bin2hex((string) $version1));
    }
}
