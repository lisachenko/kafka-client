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
 * Heartbeat response, version 0.
 *
 * <pre>
 *   Heartbeat Response (Version: 0) => error_code
 *     error_code => INT16
 * </pre>
 *
 * The error code is the whole answer, and it is how the coordinator tells a member what to do next: 27
 * (RebalanceInProgress) means the group is rebalancing and the member has to send JoinGroup again, 25
 * (UnknownMemberId) that it was dropped out of the group, 22 (IllegalGeneration) that its generation is over.
 *
 * @see docs/protocol/0.11.0.md, section "Heartbeat API (key 12, v0)"
 */
class HeartbeatResponse extends AbstractResponse
{
    /**
     * Error code.
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
