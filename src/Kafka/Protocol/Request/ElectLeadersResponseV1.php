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
 * ElectLeaders response of version 1 (Kafka 2.4, KIP-460), the body of version 2 before KIP-482
 *
 * <pre>
 *   ElectLeaders Response (Version: 1) => throttle_time_ms error_code [replica_election_results]
 * </pre>
 *
 * The top-level error code of KIP-460 is in both versions; the version 2 writes the same fields compactly and ends
 * every structure in a tagged-field section.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
final class ElectLeadersResponseV1 extends ElectLeadersResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
