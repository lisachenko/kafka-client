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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;

/**
 * This request queries the supported SASL mechanisms on the broker
 */
class SaslHandshakeRequest extends AbstractRequest
{
    /**
     * @param string $mechanism
     */
    public function __construct(/**
     * SASL Mechanism chosen by the client.
     */
        private $mechanism,
        $clientId = '',
        $correlationId = 0
    ) {
        parent::__construct(ApiKeys::SASL_HANDSHAKE, $clientId, $correlationId, ApiKeys::VERSION_0);
    }

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload         = parent::packPayload();
        $mechanismLength = strlen($this->mechanism);

        $payload .= pack("na{$mechanismLength}", $mechanismLength, $this->mechanism);

        return $payload;
    }
}
