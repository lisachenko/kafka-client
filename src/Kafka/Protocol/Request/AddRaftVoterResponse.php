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
 * AddRaftVoter response object, version 1 (key 80, Kafka 3.9, KIP-853)
 *
 * <pre>
 *   AddRaftVoter Response (Version: 0 to 1) => throttle_time_ms error_code error_message
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     error_message    => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * The whole answer is the outcome: 0 once the new voter set is **committed** by the quorum, or the code of the
 * check that stopped the request with the sentence the raft client wrote for it - 104, 42, 35, 7, 126 or, when
 * the new voter could not be asked, the 7 of an aborted operation (see {@see AddRaftVoterRequest}).
 *
 * **Version 1 (Kafka 4.2) is the frame of version 0**: "Version 1 is the same as version 0" is the comment above the
 * `validVersions` of `AddRaftVoterResponse.json` @ 4.2.0. What the `ack_when_committed` of the request changes is
 * *when* the 0 is written - once the new voter set is committed, or as soon as the leader has appended it - never
 * the frame. {@see AddRaftVoterResponseV0} is the answer of a {@see AddRaftVoterRequestV0}.
 *
 * @see docs/protocol/4.3.md, sections "AddRaftVoter API (key 80, v0 and v1)" and "The acknowledgement mode of
 *      Kafka 4.2 (v1)"
 */
class AddRaftVoterResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

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
