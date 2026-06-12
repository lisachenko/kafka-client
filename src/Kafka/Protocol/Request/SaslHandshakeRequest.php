<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * This request queries the supported SASL mechanisms on the broker
 */
class SaslHandshakeRequest extends AbstractRequest
{
    public function __construct(/**
     * SASL Mechanism chosen by the client.
     */
        private readonly string $mechanism,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(ApiKeys::SASL_HANDSHAKE, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = null;

        return $header + [
            'mechanism' => BinarySchema::TYPE_STRING,
        ];
    }
}
