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
 * SyncGroup answer of version 4 (Kafka 2.4, KIP-482): the flexible frame without the two fields of KIP-559
 *
 * Version 5 (Kafka 2.5) put a nullable `protocol_type` and `protocol_name` between the error code and the
 * assignment, with which the coordinator tells a member what the group has settled on.
 *
 * @see docs/protocol/2.8.md, section "The protocol type and name of KIP-559 (Kafka 2.5)"
 * @see docs/protocol/2.8.md, section "SyncGroup API (key 14, v0 to v5)"
 */
final class SyncGroupResponseV4 extends SyncGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
