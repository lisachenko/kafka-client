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
 * @date 27.07.2014
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;

/**
 * This request asks for the controlled shutdown of specific broker
 */
class ControlledShutdownRequest extends AbstractRequest
{
    /**
     * @param int $brokerId
     */
    public function __construct(/**
     * Broker identifier to shutdown
     */
        private $brokerId,
        $clientId = '',
        $correlationId = 0
    ) {
        parent::__construct(ApiKeys::CONTROLLED_SHUTDOWN, $clientId, $correlationId, ApiKeys::VERSION_1);
    }

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload = parent::packPayload();

        $payload .= pack('N', $this->brokerId);

        return $payload;
    }
}
