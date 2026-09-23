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
 * JoinGroup request of version 8 (Kafka 3.2, KIP-800): the first frame that carries the `reason`
 *
 * Version 9 (KIP-814, Kafka 3.2) sends the very same bytes one api version higher - "Version 9 is the same as
 * version 8" in `JoinGroupRequest.json` @ 3.2.3 - and what it buys is in the **answer**, whose `skip_assignment`
 * tells a returning static leader that the group keeps the assignment it already has. A member that sends this
 * version is answered with {@see JoinGroupResponseV8}, which has no such flag, so its leader always computes the
 * assignment itself.
 *
 * @see docs/protocol/4.3.md, section "The reason of KIP-800 and the skip_assignment of KIP-814 (v8 and v9)"
 * @see docs/protocol/4.3.md, section "JoinGroup API (key 11, v0 to v9)"
 */
final class JoinGroupRequestV8 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
