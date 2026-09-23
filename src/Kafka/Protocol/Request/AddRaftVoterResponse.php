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
 * AddRaftVoter response object, version 0 (key 80, Kafka 3.9, KIP-853)
 *
 * <pre>
 *   AddRaftVoter Response (Version: 0) => throttle_time_ms error_code error_message
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     error_message    => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * The whole answer is the outcome: 0 once the new voter set is **committed** by the quorum, or the code of the
 * check that stopped the request with the sentence the raft client wrote for it - 104, 42, 35, 7, 126 or, when
 * the new voter could not be asked, the 7 of an aborted operation (see {@see AddRaftVoterRequest}).
 *
 * @see docs/protocol/4.3.md, section "AddRaftVoter API (key 80, v0)"
 */
class AddRaftVoterResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the request, 0 when the voter was added
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
