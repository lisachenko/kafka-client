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
 * FindCoordinator answer of version 5 (Kafka 3.8, KIP-890): the `coordinators` array before the share groups
 *
 * Version 6 (Kafka 3.9, KIP-932) changed no field - it is what lets the coordinator type
 * {@see GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE} be asked for at all - so this answer holds the very
 * bytes of a version 6 answer. {@see GroupCoordinatorResponse} decodes the version 6.
 *
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v6)"
 */
final class GroupCoordinatorResponseV5 extends GroupCoordinatorResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
