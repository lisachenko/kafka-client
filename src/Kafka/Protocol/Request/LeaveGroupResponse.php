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
 * LeaveGroup response, version 0.
 *
 * <pre>
 *   LeaveGroup Response (Version: 0) => error_code
 *     error_code => INT16
 * </pre>
 *
 * A member id the coordinator does not know - because the member was already removed, or because the group does not
 * exist at all - is answered with the error code 25 (UnknownMemberId).
 *
 * @see docs/protocol/0.10.2.md, section "LeaveGroup API (key 13, v0)"
 */
class LeaveGroupResponse extends AbstractResponse
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
