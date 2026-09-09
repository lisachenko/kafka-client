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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;

/**
 * SASL handshake response
 *
 * <pre>
 *   SaslHandshakeResponse => ErrorCode [EnabledMechanisms]
 *     ErrorCode         => int16
 *     EnabledMechanisms => string
 * </pre>
 *
 * The error code is 0 when the broker accepted the mechanism of the request, and 33 (UnsupportedSaslMechanism) when
 * it did not; `EnabledMechanisms` always carries the `sasl.enabled.mechanisms` of the broker, so a client can report
 * what it could have asked for. After an error the broker closes the connection instead of waiting for another
 * handshake (`SaslServerAuthenticator.handleKafkaRequest` @ 0.10.2.2).
 *
 * @see docs/protocol/0.10.2.md, section "Transport security (SSL)", subsection "SASL/PLAIN"
 */
class SaslHandshakeResponse extends AbstractResponse
{
    /**
     * Error code.
     */
    public int $errorCode;

    /**
     * Array of mechanisms enabled in the server.
     *
     * @var list<string>
     */
    public array $enabledMechanisms = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'         => BinarySchema::TYPE_INT16,
            'enabledMechanisms' => [BinarySchema::TYPE_STRING],
        ];
    }
}
