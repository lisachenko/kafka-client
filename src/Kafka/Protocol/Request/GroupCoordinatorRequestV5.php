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
 * FindCoordinator request of version 5 (Kafka 3.8, KIP-890): the batched lookup before the share groups
 *
 * Version 6 (Kafka 3.9, KIP-932) added no field to either half of the api either - *"Version 6 adds support for
 * share groups (KIP-932)"* - so this frame is the version 6 frame with the number 5 in its header, and it is the
 * highest version a broker below Kafka 3.9 serves. {@see GroupCoordinatorRequest} sends the version 6.
 *
 * What the number below the 6 costs is the coordinator type **2**: `KafkaApis.getCoordinator` @ 3.9.2 refuses
 * {@see GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE} with the error code 42 (InvalidRequest) while
 * `apiVersion < 6`, measured on the node with this very class.
 *
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v6)"
 */
final class GroupCoordinatorRequestV5 extends GroupCoordinatorRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
