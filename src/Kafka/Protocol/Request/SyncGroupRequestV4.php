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
 * SyncGroup request of version 4 (Kafka 2.4, KIP-482): the first flexible frame of the api
 *
 * Version 5 (Kafka 2.5, KIP-559) appended the nullable `protocol_type` and `protocol_name` behind the
 * `group_instance_id`, and a **5 is refused with 23** (`InconsistentGroupProtocol`) when either of them is null or
 * does not match what the coordinator knows about the group - `KafkaApis.handleSyncGroupRequest` @ 2.8.2 checks
 * `areMandatoryProtocolTypeAndNamePresent()` before anything else. This version names neither and is accepted by
 * any generation.
 *
 * @see docs/protocol/2.8.md, section "The protocol type and name of KIP-559 (Kafka 2.5)"
 * @see docs/protocol/2.8.md, section "SyncGroup API (key 14, v0 to v5)"
 */
final class SyncGroupRequestV4 extends SyncGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
