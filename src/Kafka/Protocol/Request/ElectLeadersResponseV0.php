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

/**
 * ElectLeaders response of version 0 (Kafka 2.2), the frame of version 1 WITHOUT the top-level error code
 *
 * <pre>
 *   ElectLeaders Response (Version: 0) => throttle_time_ms [replica_election_results]
 * </pre>
 *
 * `ElectLeadersResponse` @ 2.8.2 writes the top-level `error_code` only `if (version >= 1)`, so this frame reports
 * everything per partition - the 31 `ClusterAuthorizationFailed` of a refused client included, which arrives on
 * every partition the request named instead of once.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
final class ElectLeadersResponseV0 extends ElectLeadersResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
