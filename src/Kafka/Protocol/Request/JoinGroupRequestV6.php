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
 * JoinGroup request of version 6 (Kafka 2.4, KIP-482): the first flexible frame of the api
 *
 * Version 7 (Kafka 2.5, KIP-559) sends the very same bytes one api version higher - "Version 7 is the same as
 * version 6" in `JoinGroupRequest.json` @ 2.8.2 - and what it buys is in the **answer**, which carries the
 * `protocol_type` of the group back and may report a null `protocol_name`. A member that sends this version is
 * answered with {@see JoinGroupResponseV6}, whose answer has neither.
 *
 * @see docs/protocol/2.8.md, section "The protocol type and name of KIP-559 (Kafka 2.5)"
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v7)"
 */
final class JoinGroupRequestV6 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
