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

namespace Protocol\Kafka\Tests\Unit\Common\Errors;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\ClientExceptionInterface;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\SaslAuthenticationException;
use Protocol\Kafka\Common\Errors\SaslAuthenticationFailedException;
use Protocol\Kafka\Common\Errors\ServerExceptionInterface;

/**
 * The client-side exception of a failed SASL authentication and its context.
 *
 * The two exceptions of a refused credential are easy to confuse, and the epic decided their names deliberately:
 * {@see SaslAuthenticationFailedException} is the **wire code 58** of Kafka 1.0 (`SaslAuthenticationException` in
 * the Java client), while {@see SaslAuthenticationException} is what this package raises to the application and
 * has raised since the 0.10 line - it carries the code as its cause when there is one.
 *
 * @see docs/protocol/1.1.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
 */
#[CoversClass(SaslAuthenticationException::class)]
#[CoversClass(SaslAuthenticationFailedException::class)]
final class SaslAuthenticationExceptionTest extends TestCase
{
    /**
     * The context of the framed exchange: the code, the message of the broker and the cause that carries the code
     */
    public function testContextOfARefusedCredentialCarriesTheAnswerOfTheBroker(): void
    {
        $message   = 'Authentication failed: Invalid username or password';
        $exception = new SaslAuthenticationException(
            [
                'error'        => $message,
                'errorCode'    => KafkaException::SASL_AUTHENTICATION_FAILED,
                'errorMessage' => $message,
                'mechanism'    => 'PLAIN',
                'username'     => 'kafkatest',
                'host'         => '127.0.0.1',
                'port'         => 9094,
            ],
            new SaslAuthenticationFailedException(['errorMessage' => $message])
        );

        self::assertSame(58, $exception->getContext()['errorCode']);
        self::assertSame($message, $exception->getContext()['errorMessage']);
        self::assertSame('kafkatest', $exception->getContext()['username']);
        self::assertStringContainsString($message, $exception->getMessage());
        self::assertStringContainsString('"port":9094', $exception->getMessage(), 'the context is in the message');
        self::assertInstanceOf(SaslAuthenticationFailedException::class, $exception->getPrevious());
    }

    /**
     * Without a message of the broker - the raw exchange of a v0 handshake - the exception says so itself
     */
    public function testContextOfAClosedConnectionHasNoErrorCode(): void
    {
        $exception = new SaslAuthenticationException([], new NetworkException(['error' => 'closed']));

        self::assertArrayNotHasKey('errorCode', $exception->getContext());
        self::assertStringContainsString('the broker refused the credentials', $exception->getMessage());
        self::assertInstanceOf(NetworkException::class, $exception->getPrevious());
    }

    /**
     * It has to leave the retry loops of the client, which is why it is not a {@see KafkaException} at all
     */
    public function testItIsAClientExceptionAndNotARetriableKafkaException(): void
    {
        $exception = new SaslAuthenticationException(['error' => 'no']);

        self::assertInstanceOf(ClientExceptionInterface::class, $exception);
        self::assertNotInstanceOf(KafkaException::class, $exception);
    }

    /**
     * The wire code, on the other hand, is an ordinary server-side error code of the protocol
     */
    public function testTheWireCodeIsTheServerSideExceptionOfTheErrorCode58(): void
    {
        $failed = new SaslAuthenticationFailedException(['mechanism' => 'PLAIN']);

        self::assertInstanceOf(KafkaException::class, $failed);
        self::assertInstanceOf(ServerExceptionInterface::class, $failed);
        self::assertSame(KafkaException::SASL_AUTHENTICATION_FAILED, $failed->getCode());
        self::assertInstanceOf(
            SaslAuthenticationFailedException::class,
            KafkaException::fromCode(KafkaException::SASL_AUTHENTICATION_FAILED)
        );
    }
}
