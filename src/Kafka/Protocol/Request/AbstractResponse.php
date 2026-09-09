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

use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Basic class for all responses
 *
 * Response Header => correlation_id
 *   correlation_id => INT32
 *
 * @see docs/protocol/0.10.2.md, section "Responses"
 */
abstract class AbstractResponse extends AbstractProtocolMessage
{
    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'messageSize'   => BinarySchema::TYPE_INT32,
            'correlationId' => BinarySchema::TYPE_INT32,
        ];
    }

    /**
     * Returns the correlation id that the broker echoed back from the matching request
     */
    public function getCorrelationId(): int
    {
        return $this->correlationId;
    }
}
