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
 * A Kafka 0.10 broker has no error code for this: `SaslServerAuthenticator.authenticate()` @ 0.10.2.2 lets the
 * `SaslException` of the mechanism bubble up as an `IOException` and the connection is simply closed, without an
 * answer - only the handshake that precedes the tokens has an error code of its own (33 UnsupportedSaslMechanism).
 * The dedicated error code 58 (SaslAuthenticationFailed) and the `SaslAuthenticate` request that carries it arrived
 * with Kafka 1.0, so a client of this line can only report the closed connection.
 *
 * It is deliberately *not* a {@see KafkaException}, exactly like {@see InvalidConfigurationException}: the cause is
 * a wrong user name or password, not a state of the cluster, so it has to leave the retry loops of the client -
 * {@see \Protocol\Kafka\Common\Cluster::bootstrap()} and {@see \Protocol\Kafka\Network\RetryPolicy} - instead of
 * being attempted again until a timeout runs out. Nothing about the connection will be different next time.
 *
 * @see docs/protocol/0.11.0.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
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
