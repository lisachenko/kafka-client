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

namespace Protocol\Kafka\Common\Errors;

use RuntimeException;
use Throwable;

/**
 * The SASL authentication of a connection failed.
 *
 * This is the client-side exception the socket layer raises for every way an authentication can end badly, and it
 * carries what the broker said in its context. What that is depends on the version of the `SaslHandshake` request
 * the connection opened with, and Kafka 1.0 (KIP-152) is the release that gave it words:
 *
 * * after a **v1** handshake the tokens travel inside `SaslAuthenticate` requests, and refused credentials come
 *   back as the error code **58** with a message - `Authentication failed: Invalid username or password` on a
 *   1.1.1 broker. The context then carries `errorCode`, the broker's `errorMessage`, and a
 *   {@see SaslAuthenticationFailedException} - the wire code - as the cause;
 * * after a **v0** handshake the tokens are raw frames and the broker has no way to answer at all: it closes the
 *   connection in the middle of the exchange (`SaslException` -> `IOException`), and the cause is the
 *   {@see NetworkException} of the dropped socket. That is the whole story on every line up to 0.11.
 *
 * The Java client calls the wire code `SaslAuthenticationException`; here that name belongs to this class, and the
 * code is {@see SaslAuthenticationFailedException}.
 *
 * It is deliberately *not* a {@see KafkaException}, exactly like {@see InvalidConfigurationException}: the cause is
 * a wrong user name or password, not a state of the cluster, so it has to leave the retry loops of the client -
 * {@see \Protocol\Kafka\Common\Cluster::bootstrap()} and {@see \Protocol\Kafka\Network\RetryPolicy} - instead of
 * being attempted again until a timeout runs out. Nothing about the connection will be different next time.
 *
 * @see docs/protocol/2.8.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
 */
class SaslAuthenticationException extends RuntimeException implements ClientExceptionInterface
{
    /**
     * @param array<string, mixed> $context  What was attempted, never the password itself
     * @param Throwable|null       $previous The dropped connection this was diagnosed from, if there is one
     */
    public function __construct(private readonly array $context = [], ?Throwable $previous = null)
    {
        $error = isset($context['error']) ? (string) $context['error'] : 'the broker refused the credentials';

        parent::__construct(
            "The SASL authentication of the connection failed: {$error}." . PHP_EOL
            . 'Context: ' . json_encode($context),
            0,
            $previous
        );
    }

    /**
     * Returns the context for this exception
     *
     * @return array<string, mixed>
     */
    public function getContext(): array
    {
        return $this->context;
    }
}
