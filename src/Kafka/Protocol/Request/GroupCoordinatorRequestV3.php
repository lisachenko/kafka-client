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
 * FindCoordinator request of version 3 (Kafka 2.4, KIP-482): one key per request, flexibly encoded
 *
 * Version 4 (Kafka 3.0, KIP-699) replaced the single `key` with an array of `coordinator_keys`, so that one
 * request can look several coordinators of the same type up at once; this version is the last one that carries a
 * single key and the highest one a broker below Kafka 3.0 serves. {@see GroupCoordinatorRequest} sends the batched
 * frame.
 *
 * @see docs/protocol/3.9.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v4)"
 */
final class GroupCoordinatorRequestV3 extends GroupCoordinatorRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
