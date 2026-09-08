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
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture\EmptyResponse;
use Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture\StubRequest;
use Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture\StubResponse;

/**
 * Byte-exact tests for the request and response framing.
 *
 * @see docs/protocol/0.8.2.md, sections "Requests" and "Responses"
 */
#[CoversClass(AbstractProtocolMessage::class)]
#[CoversClass(AbstractRequest::class)]
#[CoversClass(AbstractResponse::class)]
final class FramingTest extends TestCase
{
    /**
     * MetadataRequest v0, correlation id 1, client id "test", empty topic list.
     *
     *   Size          => 00 00 00 12 (18 bytes)
     *   ApiKey        => 00 03
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   [TopicName]   => 00 00 00 00
     */
    private const string METADATA_REQUEST_HEX = '00000012'
        . '0003'
        . '0000'
        . '00000001'
        . '0004' . '74657374'
        . '00000000';

    public function testRequestHeaderIsPackedAccordingToTheSpec(): void
    {
        $request = new MetadataRequest([], 'test', 1);

        self::assertSame(self::METADATA_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testRequestIsWrittenToTheStreamPrefixedWithItsSize(): void
    {
        $buffer  = '';
        $stream  = new StringStream($buffer);
        $request = new MetadataRequest([], 'test', 1);
        $request->writeTo($stream);

        self::assertSame(self::METADATA_REQUEST_HEX, bin2hex($buffer));
    }

    public function testTypedBodyContractProducesTheSameBytesAsTheLegacyOne(): void
    {
        $legacy = new MetadataRequest([], 'test', 1);
        $typed  = new StubRequest([], 'test', 1);

        self::assertSame(bin2hex((string) $legacy), bin2hex((string) $typed));
        self::assertSame(ApiKeys::METADATA, $typed->getApiKey());
        self::assertSame(0, $typed->getApiVersion());
        self::assertSame(1, $typed->getCorrelationId());
        self::assertSame('test', $typed->getClientId());
    }

    public function testRequestBodyIsWrittenAfterTheHeader(): void
    {
        $request = new StubRequest(['foo'], '', 0);

        self::assertSame(
            '00000013'
            . '0003'
            . '0000'
            . '00000000'
            . '0000'
            . '00000001' . '0003' . '666f6f',
            bin2hex((string) $request)
        );
    }

    public function testEmptyClientIdIsWrittenAsAnEmptyStringNotAsNull(): void
    {
        $request = new MetadataRequest([], '', 7);

        self::assertSame(
            '0000000e' . '0003' . '0000' . '00000007' . '0000' . '00000000',
            bin2hex((string) $request)
        );
    }

    public function testCorrelationIdIsTakenFromTheCallerAndNotFromAHiddenCounter(): void
    {
        $first  = new MetadataRequest([], 'test', 42);
        $second = new MetadataRequest([], 'test', 42);

        self::assertSame(42, $first->getCorrelationId());
        self::assertSame(42, $second->getCorrelationId());
        self::assertSame(bin2hex((string) $first), bin2hex((string) $second));
    }

    public function testCorrelationIdHelperProducesAMonotonicSequence(): void
    {
        $first  = AbstractRequest::nextCorrelationId();
        $second = AbstractRequest::nextCorrelationId();

        self::assertSame($first + 1, $second);
    }

    public function testResponseHeaderIsUnpackedAccordingToTheSpec(): void
    {
        // Size = 12 bytes: CorrelationId + ErrorCode + TopicName
        $frame = hex2bin('0000000c' . '00000001' . '0003' . '0004' . '74657374');

        $response = StubResponse::unpackFrom(StringStream::fromString($frame));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(3, $response->errorCode);
        self::assertSame('test', $response->topic);
    }

    public function testResponseBodyIsBoundedByTheAnnouncedSize(): void
    {
        $frame  = hex2bin('0000000c' . '00000001' . '0003' . '0004' . '74657374');
        $stream = StringStream::fromString($frame . 'trailing bytes');

        StubResponse::unpackFrom($stream);

        self::assertSame('trailing bytes', $stream->readRaw(14));
    }

    public function testResponseWithoutABodyOnlyCarriesTheCorrelationId(): void
    {
        $response = EmptyResponse::unpackFrom(StringStream::fromString(hex2bin('00000004' . '0000002a')));

        self::assertSame(42, $response->getCorrelationId());
    }

    public function testTruncatedResponseSizeIsRejected(): void
    {
        $this->expectException(NetworkException::class);
        StubResponse::unpackFrom(StringStream::fromString(hex2bin('00000003')));
    }
}
