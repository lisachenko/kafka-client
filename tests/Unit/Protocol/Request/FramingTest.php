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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture\EmptyResponse;
use Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture\ErrorCodeResponse;
use Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture\SchemaMetadataRequest;
use Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture\SchemaMetadataResponse;

/**
 * Byte-exact tests for the request and response framing.
 *
 * @see docs/protocol/0.10.2.md, sections "Requests" and "Responses"
 */
#[CoversClass(AbstractProtocolMessage::class)]
#[CoversClass(AbstractRequest::class)]
#[CoversClass(AbstractResponse::class)]
final class FramingTest extends TestCase
{
    /**
     * Metadata request v0, correlation id 1, client id "test", empty topic list.
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
        $request = new SchemaMetadataRequest([], 'test', 1);

        self::assertSame(self::METADATA_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testRequestIsWrittenToTheStreamPrefixedWithItsSize(): void
    {
        $stream  = new StringStream();
        $request = new SchemaMetadataRequest([], 'test', 1);
        $request->writeTo($stream);

        self::assertSame(self::METADATA_REQUEST_HEX, bin2hex($stream->getBuffer()));
        // Size counts everything that follows it: the 18 bytes of header and body, not the size field
        self::assertSame(18, $request->getMessageSize());
    }

    public function testRequestSchemeDescribesTheHeaderOfTheSpec(): void
    {
        self::assertSame(
            [
                'messageSize'   => BinarySchema::TYPE_INT32,
                'apiKey'        => BinarySchema::TYPE_INT16,
                'apiVersion'    => BinarySchema::TYPE_INT16,
                'correlationId' => BinarySchema::TYPE_INT32,
                'clientId'      => BinarySchema::TYPE_STRING,
            ],
            AbstractRequest::getScheme()
        );
    }

    public function testSubclassSchemeExtendsTheHeaderOfTheParent(): void
    {
        $scheme = SchemaMetadataRequest::getScheme();

        self::assertSame(array_keys(AbstractRequest::getScheme()), array_slice(array_keys($scheme), 0, 5));
        self::assertSame([BinarySchema::TYPE_STRING], $scheme['topics']);
    }

    public function testATestFixtureAndTheRealRequestOfTheSameApiProduceTheSameBytes(): void
    {
        $real  = new MetadataRequest([], 'test', 1);
        $typed = new SchemaMetadataRequest([], 'test', 1);

        self::assertSame(bin2hex((string) $real), bin2hex((string) $typed));
        self::assertSame(ApiKeys::METADATA, $typed->getApiKey());
        self::assertSame(0, $typed->getApiVersion());
        self::assertSame(1, $typed->getCorrelationId());
        self::assertSame('test', $typed->getClientId());
    }

    public function testRequestBodyIsWrittenAfterTheHeader(): void
    {
        $request = new SchemaMetadataRequest(['foo'], '', 0);

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
        $request = new SchemaMetadataRequest([], '', 7);

        self::assertSame(
            '0000000e' . '0003' . '0000' . '00000007' . '0000' . '00000000',
            bin2hex((string) $request)
        );
    }

    public function testCorrelationIdIsTakenFromTheCallerAndNotFromAHiddenCounter(): void
    {
        $first  = new SchemaMetadataRequest([], 'test', 42);
        $second = new SchemaMetadataRequest([], 'test', 42);

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

    public function testResponseSchemeDescribesTheHeaderOfTheSpec(): void
    {
        self::assertSame(
            [
                'messageSize'   => BinarySchema::TYPE_INT32,
                'correlationId' => BinarySchema::TYPE_INT32,
            ],
            AbstractResponse::getScheme()
        );
    }

    public function testResponseIsUnpackedThroughItsScheme(): void
    {
        // Size = 29: CorrelationId + [Broker] with one entry (nodeId, "127.0.0.1", port) + ErrorCode
        $frame = hex2bin(
            '0000001d'
            . '00000001'
            . '00000001' . '00000000' . '0009' . bin2hex('127.0.0.1') . '00002384'
            . '0000'
        );

        $response = SchemaMetadataResponse::unpack(new StringStream($frame));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame([0], array_keys($response->brokers));
        self::assertSame('127.0.0.1', $response->brokers[0]->host);
        self::assertSame(9092, $response->brokers[0]->port);
        self::assertSame(0, $response->errorCode);
    }

    public function testResponseBodyIsBoundedByTheAnnouncedSize(): void
    {
        // The frame announces six bytes; whatever follows them belongs to the next response on that connection
        $frame  = hex2bin('00000006' . '00000001' . '0003');
        $stream = new StringStream($frame . 'trailing bytes');

        $response = ErrorCodeResponse::unpack($stream);

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(3, $response->errorCode);
        self::assertSame('trailing bytes', $stream->readRaw(14));
    }

    public function testResponseErrorCodesAreReadAsSignedValues(): void
    {
        $response = ErrorCodeResponse::unpack(new StringStream(hex2bin('00000006' . '0000002a' . 'ffff')));

        self::assertSame(42, $response->getCorrelationId());
        self::assertSame(-1, $response->errorCode, 'the error code -1 (Unknown) is a negative int16');
    }

    public function testResponseWithoutABodyOnlyCarriesTheCorrelationId(): void
    {
        $response = EmptyResponse::unpack(new StringStream(hex2bin('00000004' . '0000002a')));

        self::assertSame(42, $response->getCorrelationId());
        self::assertSame(4, $response->getMessageSize());
    }

    public function testOversizedFrameIsRejected(): void
    {
        $this->expectException(NetworkException::class);
        EmptyResponse::unpack(new StringStream(hex2bin('7fffffff')));
    }
}
