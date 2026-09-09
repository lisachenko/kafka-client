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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntry;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResource;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequest;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponse;

/**
 * Byte-exact tests for the DescribeConfigs API of Kafka 0.11 (api key 32, v0).
 *
 * @see docs/protocol/0.11.0.md, section "DescribeConfigs API (key 32, v0)"
 */
#[CoversClass(DescribeConfigsRequest::class)]
#[CoversClass(DescribeConfigsResponse::class)]
#[CoversClass(DescribeConfigsRequestResource::class)]
#[CoversClass(DescribeConfigsResponseResource::class)]
#[CoversClass(DescribeConfigsResponseConfigEntry::class)]
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
     * The entry of a sensitive option: `is_sensitive = 01` and a value of `ff ff`, the null of a NULLABLE_STRING
     */
    private const string SENSITIVE_RESPONSE_HEX = '00000034'
        . '00000007'
        . '00000000'
        . '00000001'
        . '0000' . 'ffff' . '04' . '0001' . '30'
        . '00000001' . '0015' . '73736c2e6b657973746f72652e70617373776f7264' . 'ffff' . '01' . '00' . '01';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DescribeConfigsRequest(
            [
                new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', ['retention.ms']),
                DescribeConfigsRequestResource::fromConfigResource(ConfigResource::broker(0)),
            ],
            'test',
            7
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_CONFIGS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'a 0.11.0.3 broker only serves version 0');
    }

    public function testANullConfigNameArrayIsTheCountMinusOne(): void
    {
        $all = new DescribeConfigsRequest(
            [new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', null)],
            'test',
            7
        );
        $none = new DescribeConfigsRequest(
            [new DescribeConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', [])],
            'test',
            7
        );

        // "every option" and "no option at all" are the same four bytes apart: ff ff ff ff against 00 00 00 00
        self::assertStringEndsWith('ffffffff', bin2hex((string) $all));
        self::assertStringEndsWith('00000000', bin2hex((string) $none));
        self::assertSame(strlen((string) $all), strlen((string) $none));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

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

        self::assertSame(KafkaException::INVALID_REQUEST, $broker->errorCode);
        self::assertSame('bad', $broker->errorMessage);
        self::assertSame(ConfigResource::TYPE_BROKER, $broker->resourceType);
        self::assertSame([], $broker->configEntries, 'a refused resource carries no entry at all');
    }

    public function testTheValueOfASensitiveOptionIsNull(): void
    {
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::SENSITIVE_RESPONSE_HEX)));

        $entry = $response->resources[0]->configEntries['ssl.keystore.password'];

        self::assertTrue($entry->isSensitive);
        self::assertNull($entry->configValue, 'the broker never sends the value of a PASSWORD option');
        self::assertTrue($entry->readOnly, 'every option of a broker resource is read only on Kafka 0.11');
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $response = DescribeConfigsResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }
}
