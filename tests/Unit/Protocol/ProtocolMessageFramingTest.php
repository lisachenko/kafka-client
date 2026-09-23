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

namespace Protocol\Kafka\Tests\Unit\Protocol;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequest;

/**
 * A protocol message reaches its stream as ONE write
 *
 * The schema writer emits a message field by field. A socket stream can replace a connection the broker closed
 * only before the first byte of a frame has left it ({@see \Protocol\Kafka\IO\SocketStream::write()}); a frame
 * that continued on a new connection would be read by the broker from its middle, as a size field it then waits
 * on forever, and every later request of the process would wait with it. `writeTo()` therefore packs the whole
 * frame first and hands it to the stream in a single call, so that the stream can always tell "nothing sent yet"
 * from "half a frame sent".
 */
#[CoversClass(AbstractProtocolMessage::class)]
final class ProtocolMessageFramingTest extends TestCase
{
    public function testAMessageIsWrittenToTheStreamInOneCall(): void
    {
        $stream  = new WriteCountingStream();
        $request = new ApiVersionsRequest('kafka-client-test', 42);

        $request->writeTo($stream);

        self::assertSame(1, $stream->writes, 'the whole frame in one write() call');
        self::assertSame((string) $request, $stream->getBuffer(), 'and the bytes are the ones of the message');
    }

    public function testAFlexibleAndAPlainMessageAreFramedTheSameWay(): void
    {
        $flexible = new WriteCountingStream();
        $plain    = new WriteCountingStream();

        new ApiVersionsRequest('kafka-client-test', 1)->writeTo($flexible);
        new MetadataRequest(['test'], true, 'kafka-client-test', 2)->writeTo($plain);

        self::assertSame(1, $flexible->writes);
        self::assertSame(1, $plain->writes);
        self::assertSame(
            strlen($flexible->getBuffer()) - 4,
            (int) unpack('N', substr($flexible->getBuffer(), 0, 4))[1],
            'the size field counts every byte behind it'
        );
    }
}

/**
 * A string stream that counts the write() calls it receives
 */
final class WriteCountingStream extends StringStream
{
    public int $writes = 0;

    public function write(string $format, ...$arguments): void
    {
        $this->writes++;
        parent::write($format, ...$arguments);
    }
}
