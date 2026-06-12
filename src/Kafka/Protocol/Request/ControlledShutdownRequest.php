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
 * This request asks for the controlled shutdown of specific broker
 *
 * ControlledShutdown Request (Version: 0) => broker_id
 *   broker_id => INT32
 */
class ControlledShutdownRequest extends AbstractRequest
{
    /**
     * @inheritDoc
     */
    protected const VERSION = 1;

    public function __construct(/**
     * Broker identifier to shutdown
     */
        private readonly int $brokerId,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(ApiKeys::CONTROLLED_SHUTDOWN, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = null;

        return $header + [
            'brokerId' => BinarySchema::TYPE_INT32,
        ];
    }
}
