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
 * JoinGroup request of version 7 (Kafka 2.5, KIP-559): the last frame without the `reason` of KIP-800
 *
 * Version 8 (Kafka 3.2, KIP-800) appended a nullable `reason` behind the protocol array, "the reason why the
 * member (re-)joins the group" - `JoinGroupRequest.json` @ 3.2.3 - which the coordinator logs and nothing more.
 * This is the same request without that field, and the {@see JoinGroupRequest::$reason} of an instance of this
 * class never reaches the wire.
 *
 * @see docs/protocol/3.9.md, section "The reason of KIP-800 and the skip_assignment of KIP-814 (v8 and v9)"
 * @see docs/protocol/3.9.md, section "JoinGroup API (key 11, v0 to v9)"
 */
final class JoinGroupRequestV7 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
