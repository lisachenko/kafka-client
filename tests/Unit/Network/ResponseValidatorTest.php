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

namespace Protocol\Kafka\Tests\Unit\Network;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;

/**
 * Tests that an answer is only accepted when it carries the correlation id of the request it answers.
 *
 * @see docs/protocol/0.8.2.md, section "Responses"
 */
#[CoversClass(ResponseValidator::class)]
#[CoversClass(CorrelationIdMismatchException::class)]
final class ResponseValidatorTest extends TestCase
{
    public function testTheAnswerOfTheRequestIsAccepted(): void
    {
        $frame  = ResponseFrame::metadata(4711, [[0, 'kafka-1', 9092]]);
        $stream = new StringStream($frame);

        $response = ResponseValidator::read(MetadataResponse::class, $stream, 4711);

        self::assertInstanceOf(MetadataResponse::class, $response);
        self::assertSame(4711, $response->getCorrelationId());
        self::assertSame([0], array_keys($response->brokers));
    }

    public function testAnAnswerWithAnotherCorrelationIdIsRejected(): void
    {
        $stream = new StringStream(ResponseFrame::metadata(11, [[0, 'kafka-1', 9092]]));

        try {
            ResponseValidator::read(MetadataResponse::class, $stream, 12, ['node' => 3]);
            self::fail('A response of another request is expected to be rejected');
        } catch (CorrelationIdMismatchException $exception) {
            $context = $exception->getContext();

            self::assertSame(12, $context['expected']);
            self::assertSame(11, $context['received']);
            self::assertSame(MetadataResponse::class, $context['response']);
            self::assertSame(3, $context['node'], 'the caller may add its own context');
        }
    }

    public function testTheMismatchIsAKafkaException(): void
    {
        // The client has to be able to catch it together with the errors that the broker reports
        self::assertInstanceOf(KafkaException::class, new CorrelationIdMismatchException());
    }

    public function testAnAlreadyDecodedResponseCanBeCheckedOnItsOwn(): void
    {
        $response = MetadataResponse::unpack(new StringStream(ResponseFrame::metadata(8, [[0, 'kafka-1', 9092]])));

        ResponseValidator::assertCorrelationId(8, $response);

        $this->expectException(CorrelationIdMismatchException::class);
        ResponseValidator::assertCorrelationId(9, $response);
    }
}
