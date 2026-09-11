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
 * JoinGroup response, version 5: the generation, the leader and the members with their instance ids
 *
 * Version 6 (Kafka 2.4, KIP-482) is the same answer in the flexible encoding, see {@see JoinGroupResponse}.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v6)"
 */
final class JoinGroupResponseV5 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
