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
use Protocol\Kafka\Protocol\Data\AlterConfigsRequestConfigEntry;
use Protocol\Kafka\Protocol\Data\AlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\AlterConfigsResponseResource;
use Protocol\Kafka\Protocol\Request\AlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\AlterConfigsResponse;

/**
 * Byte-exact tests for the AlterConfigs API of Kafka 0.11 (api key 33, v0).
 *
 * @see docs/protocol/1.1.md, section "AlterConfigs API (key 33, v0)"
 */
#[CoversClass(AlterConfigsRequest::class)]
#[CoversClass(AlterConfigsResponse::class)]
#[CoversClass(AlterConfigsRequestResource::class)]
#[CoversClass(AlterConfigsRequestConfigEntry::class)]
#[CoversClass(AlterConfigsResponseResource::class)]
final class AlterConfigsTest extends TestCase
{
    /**
     * AlterConfigs request v0 that replaces the configuration of one topic with two options.
     *
     *   ApiKey        => 00 21 (33)
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test"
     *   Resources     => 00 00 00 01
     *     ResourceType => 02 (topic), ResourceName => 00 05 "topic"
     *     ConfigEntries => 00 00 00 02
     *       00 0c "retention.ms",   00 01 "1"
     *       00 0e "cleanup.policy", 00 07 "compact"
     *   ValidateOnly  => 00
     */
    private const string REQUEST_BODY_HEX = '0021'
        . '0000'
        . '00000009'
        . '0004' . '74657374'
        . '00000001'
        . '02' . '0005' . '746f706963'
        . '00000002'
        . '000c' . '726574656e74696f6e2e6d73' . '0001' . '31'
        . '000e' . '636c65616e75702e706f6c696379' . '0007' . '636f6d70616374'
        . '00';

    /**
     * AlterConfigs response v0: one resource that was altered and one that the broker refused with 42.
     *
     *   CorrelationId  => 00 00 00 09
     *   ThrottleTimeMs => 00 00 00 00
     *   Resources      => 00 00 00 02
     *     ErrorCode => 00 00, ErrorMessage => ff ff,          Type => 02, Name => 00 05 "topic"
     *     ErrorCode => 00 2a, ErrorMessage => 00 03 "bad",    Type => 04, Name => 00 01 "0"
     */
    private const string RESPONSE_BODY_HEX = '00000009'
        . '00000000'
        . '00000002'
        . '0000' . 'ffff' . '02' . '0005' . '746f706963'
        . '002a' . '0003' . '626164' . '04' . '0001' . '30';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new AlterConfigsRequest(
            [
                AlterConfigsRequestResource::fromConfigResource(
                    ConfigResource::topic('topic'),
                    ['retention.ms' => '1', 'cleanup.policy' => 'compact']
                ),
            ],
            false,
            'test',
            9
        );

        self::assertSame(self::frame(self::REQUEST_BODY_HEX), bin2hex((string) $request));
        self::assertSame(ApiKeys::ALTER_CONFIGS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'a 0.11.0.3 broker only serves version 0');
    }

    public function testValidateOnlyIsTheTrailingBooleanOfTheRequest(): void
    {
        $validate = new AlterConfigsRequest(
            [new AlterConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', ['retention.ms' => '1'])],
            true,
            'test',
            9
        );

        self::assertStringEndsWith('01', bin2hex((string) $validate));
    }

    public function testANullConfigValueIsTheNullOfANullableString(): void
    {
        // The schema of 0.11 declares the value nullable; a broker answers such an entry with the error code -1,
        // because `Properties.setProperty` throws on it - see the "AlterConfigs API" section of the document
        $request = new AlterConfigsRequest(
            [new AlterConfigsRequestResource(ConfigResource::TYPE_TOPIC, 'topic', ['retention.ms' => null])],
            false,
            'test',
            9
        );

        self::assertStringContainsString('726574656e74696f6e2e6d73' . 'ffff', bin2hex((string) $request));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = AlterConfigsResponse::unpack(new StringStream((string) hex2bin(self::frame(self::RESPONSE_BODY_HEX))));

        self::assertSame(9, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs);
        self::assertCount(2, $response->resources);

        [$topic, $broker] = $response->resources;
        self::assertSame(KafkaException::NO_ERROR, $topic->errorCode);
        self::assertNull($topic->errorMessage);
        self::assertSame(ConfigResource::TYPE_TOPIC, $topic->resourceType);
        self::assertSame('topic', $topic->resourceName);

        self::assertSame(KafkaException::INVALID_REQUEST, $broker->errorCode);
        self::assertSame('bad', $broker->errorMessage);
        self::assertSame(ConfigResource::TYPE_BROKER, $broker->resourceType);
        self::assertSame('0', $broker->resourceName);
    }

    public function testResponseSurvivesARoundTrip(): void
    {
        $frame    = self::frame(self::RESPONSE_BODY_HEX);
        $response = AlterConfigsResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame($frame, bin2hex((string) $response));
    }

    /**
     * Prefixes a body with the `Size` field it announces
     */
    private static function frame(string $bodyHex): string
    {
        return sprintf('%08x', strlen($bodyHex) / 2) . $bodyHex;
    }
}
