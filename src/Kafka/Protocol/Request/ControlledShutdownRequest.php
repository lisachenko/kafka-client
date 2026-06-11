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
    public const VERSION = 1;

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
        parent::__construct(ApiKeys::CONTROLLED_SHUTDOWN, $clientId, $correlationId);
    }

    public static function getScheme()
    {
        $header = null;

        return $header + [
            'brokerId' => BinarySchema::TYPE_INT32,
        ];
    }
}
