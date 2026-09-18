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
 * CreateDelegationToken answer of version 2, the frame before the token requester of KIP-373
 *
 * The version 3 of Kafka 3.3 added the two strings of the **requester** behind the owner
 * ({@see CreateDelegationTokenResponse}); this class is the answer without them, in which the owner is the only
 * principal there is - it is also the requester, because a version below 3 can only ask for a token of the
 * principal of its own connection. {@see CreateDelegationTokenResponseV1} is the same body in the encoding before
 * KIP-482.
 *
 * @see docs/protocol/3.9.md, section "CreateDelegationToken API (key 38, v0 to v3)"
 */
final class CreateDelegationTokenResponseV2 extends CreateDelegationTokenResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
