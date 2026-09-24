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
 * JoinGroup answer of version 7 (Kafka 2.5, KIP-559): the protocol type and name, without the flag of KIP-814
 *
 * Version 8 (Kafka 3.2, KIP-800) answers the very same bytes - "Version 8 is the same as version 7" in
 * `JoinGroupResponse.json` @ 3.2.3, the release changed the request - and version 9 (KIP-814) put the
 * `skip_assignment` of a returning static leader between the leader id and the member id, see
 * {@see JoinGroupResponse}. An answer of this version leaves {@see JoinGroupResponse::$skipAssignment} at
 * `false`, so its leader always computes the assignment itself.
 *
 * @see docs/protocol/4.3.md, section "The reason of KIP-800 and the skip_assignment of KIP-814 (v8 and v9)"
 * @see docs/protocol/4.3.md, section "JoinGroup API (key 11, v0 to v9)"
 */
final class JoinGroupResponseV7 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
