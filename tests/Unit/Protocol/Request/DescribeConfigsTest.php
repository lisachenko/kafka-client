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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntry;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntryV0;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigSynonym;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResourceV0;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequest;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponse;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponseV0;

/**
 * Byte-exact tests for the DescribeConfigs API (api key 32), version 0 of Kafka 0.11 and version 1 of Kafka 1.1.
 *
 * KIP-226 changed the config ENTRY of the answer and nothing else: the `is_default` boolean became a
 * `config_source` int8 in the same place and the entry gained its synonyms, while the request gained the trailing
 * `include_synonyms` boolean. Both versions are exercised here, and the derivation of the source from the boolean
 * of a version 0 answer with it.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 and v1)"
 */
#[CoversClass(DescribeConfigsRequest::class)]
#[CoversClass(DescribeConfigsRequestV0::class)]
#[CoversClass(DescribeConfigsResponse::class)]
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

        $request = new DescribeConfigsRequest($resources, true, 'test', 7);

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion(), 'the client sends the version 1 of Kafka 1.1');
        self::assertSame(
            strlen((string) new DescribeConfigsRequestV0($resources, 'test', 7)) + 1,
            strlen((string) $request),
            'version 1 is version 0 plus one byte'
        );
        self::assertStringEndsWith(
            '00',
            bin2hex((string) new DescribeConfigsRequest($resources, false, 'test', 7)),
            'and the flag is the last byte of the frame'
        );
    }

    public function testANullConfigNameArrayIsTheCountMinusOne(): void
    {
        $all = new DescribeConfigsRequest(
            [new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', null)],
            false,
            'test',
            7
        );
        $none = new DescribeConfigsRequest(
            [new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', [])],
            false,
            'test',
            7
        );

        // "every option" and "no option at all" are the same four bytes apart: ff ff ff ff against 00 00 00 00
        self::assertStringEndsWith('ffffffff' . '00', bin2hex((string) $all));
        self::assertStringEndsWith('00000000' . '00', bin2hex((string) $none));
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
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

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
        $response = DescribeConfigsResponse::unpack(
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
        $version1 = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $version0));
        self::assertSame(self::RESPONSE_V1_HEX, bin2hex((string) $version1));
    }
}
