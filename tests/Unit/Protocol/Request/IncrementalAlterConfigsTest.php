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

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestAlterableConfig;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsResponseResource;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequestV0;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponseV0;

/**
 * Byte-exact tests for the IncrementalAlterConfigs API of Kafka 2.3 (api key 44, v0, KIP-339).
 *
 * The api that replaces AlterConfigs: every entry of a resource carries an **operation** between the option name
 * and its value, and an option the request does not name keeps the value it has. The frames below were captured on
 * the `kafka-2-8-2` container.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0 and v1)"
 */
#[CoversClass(IncrementalAlterConfigsRequest::class)]
#[CoversClass(IncrementalAlterConfigsResponse::class)]
#[CoversClass(IncrementalAlterConfigsRequestResource::class)]
#[CoversClass(IncrementalAlterConfigsRequestAlterableConfig::class)]
#[CoversClass(IncrementalAlterConfigsResponseResource::class)]
#[CoversClass(AlterConfigOp::class)]
#[CoversClass(IncrementalAlterConfigsRequestV0::class)]
#[CoversClass(IncrementalAlterConfigsResponseV0::class)]
final class IncrementalAlterConfigsTest extends TestCase
{
    /**
     * The three operations a topic accepts, in one resource.
     *
     *   Size          => 00 00 00 71 (113 bytes)
     *   ApiKey        => 00 2c (44)
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 03 85 (901)
     *   ClientId      => 00 0a "t4-vectors"
     *   Resources     => 00 00 00 01
     *     ResourceType => 02 (topic)
     *     ResourceName => 00 0d "t4-23-vectors"
     *     Configs      => 00 00 00 03
     *       "retention.ms"   00 (SET)    00 07 "3600000"
     *       "cleanup.policy" 02 (APPEND) 00 07 "compact"
     *       "segment.bytes"  01 (DELETE) ff ff (the null value of a DELETE)
     *   ValidateOnly  => 00
     */
    private const string REQUEST_HEX = '00000071'
        . '002c'
        . '0000'
        . '00000385'
        . '000a' . '74342d766563746f7273'
        . '00000001'
        . '02'
        . '000d' . '74342d32332d766563746f7273'
        . '00000003'
        . '000c' . '726574656e74696f6e2e6d73' . '00' . '0007' . '33363030303030'
        . '000e' . '636c65616e75702e706f6c696379' . '02' . '0007' . '636f6d70616374'
        . '000d' . '7365676d656e742e6279746573' . '01' . 'ffff'
        . '00';

    /**
     * The answer of that request: one entry per resource, with a null error message
     */
    private const string RESPONSE_HEX = '00000020'
        . '00000385'
        . '00000000'
        . '00000001'
        . '0000'
        . 'ffff'
        . '02'
        . '000d' . '74342d32332d766563746f7273';

    /**
     * An APPEND to `retention.ms`, whose `ConfigDef.Type` is not LIST, and the 42 the broker answers for it
     */
    private const string INVALID_APPEND_RESPONSE_HEX = '0000005f'
        . '0000038a'
        . '00000000'
        . '00000001'
        . '002a'
        . '003f' . '436f6e6669672076616c756520617070656e64206973206e6f7420616c6c6f7765'
        . '6420666f7220636f6e666967206b65793a20726574656e74696f6e2e6d73'
        . '02'
        . '000d' . '74342d32332d766563746f7273';

    /**
     * The cluster-wide default broker resource of KIP-226 with `validate_only`, whose name is the EMPTY string
     */
    private const string BROKER_REQUEST_HEX = '0000003c'
        . '002c'
        . '0000'
        . '00000391'
        . '000a' . '74342d766563746f7273'
        . '00000001'
        . '04'
        . '0000'
        . '00000001'
        . '0008' . '6c6f672e64697273' . '00' . '000f' . '2f746d702f6b61666b612d6c6f6773'
        . '01';

    public function testTheRequestCarriesAnOperationPerOption(): void
    {
        $request = new IncrementalAlterConfigsRequestV0(
            [
                IncrementalAlterConfigsRequestResource::fromConfigResource(
                    ConfigResource::topic('t4-23-vectors'),
                    [
                        AlterConfigOp::set('retention.ms', '3600000'),
                        AlterConfigOp::append('cleanup.policy', 'compact'),
                        AlterConfigOp::delete('segment.bytes'),
                    ]
                ),
            ],
            false,
            't4-vectors',
            901
        );

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::INCREMENTAL_ALTER_CONFIGS, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion(), 'the version this line sends; v1 is the flexible one of 2.4');
    }

    public function testTheDeleteOperationIsTheOnlyOneWhoseValueIsNull(): void
    {
        $delete = AlterConfigOp::delete('segment.bytes');

        self::assertSame(AlterConfigOp::DELETE, $delete->operation);
        self::assertNull($delete->value, 'a DELETE travels with the null string as its value');
        self::assertSame('DELETE', AlterConfigOp::nameOf(AlterConfigOp::DELETE));
        self::assertSame([0, 1, 2, 3], [
            AlterConfigOp::SET,
            AlterConfigOp::DELETE,
            AlterConfigOp::APPEND,
            AlterConfigOp::SUBTRACT,
        ], 'the byte values of `AlterConfigOp.OpType` @ 2.8.2');
        self::assertFalse(AlterConfigOp::isKnown(4));
    }

    public function testAnOperationThatIsNotOneOfTheProtocolIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new AlterConfigOp('retention.ms', '1', 4);
    }

    public function testTheBrokerResourceOfKip226TravelsWithAnEmptyName(): void
    {
        $request = new IncrementalAlterConfigsRequestV0(
            [
                IncrementalAlterConfigsRequestResource::fromConfigResource(
                    ConfigResource::defaultBroker(),
                    [AlterConfigOp::set('log.dirs', '/tmp/kafka-logs')]
                ),
            ],
            true,
            't4-vectors',
            913
        );

        self::assertSame(self::BROKER_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testTheAnswerIsOneResultPerResource(): void
    {
        $response = IncrementalAlterConfigsResponseV0::unpack(
            new StringStream((string) hex2bin(self::RESPONSE_HEX))
        );

        self::assertSame(0, $response->throttleTimeMs);
        self::assertCount(1, $response->responses);
        self::assertSame(KafkaException::NO_ERROR, $response->responses[0]->errorCode);
        self::assertNull($response->responses[0]->errorMessage);
        self::assertSame(ConfigResource::TYPE_TOPIC, $response->responses[0]->resourceType);
        self::assertSame('t4-23-vectors', $response->responses[0]->resourceName);
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response));
    }

    public function testARefusedResourceCarriesTheMessageOfTheBroker(): void
    {
        $response = IncrementalAlterConfigsResponseV0::unpack(
            new StringStream((string) hex2bin(self::INVALID_APPEND_RESPONSE_HEX))
        );

        self::assertSame(KafkaException::INVALID_REQUEST, $response->responses[0]->errorCode);
        self::assertSame(
            'Config value append is not allowed for config key: retention.ms',
            $response->responses[0]->errorMessage,
            'an APPEND is only allowed for an option whose ConfigDef.Type is LIST'
        );
        self::assertSame(self::INVALID_APPEND_RESPONSE_HEX, bin2hex((string) $response));
    }
}
