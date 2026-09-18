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
 * FindCoordinator answer of version 4 (Kafka 3.0, KIP-699): the `coordinators` array before KIP-890
 *
 * Version 5 (Kafka 3.8, KIP-890) changed no field - it is the promise of the error code 120
 * `TransactionAbortable`, which a coordinator lookup never produces - so this answer holds the very bytes of a
 * version 5 answer, which {@see GroupCoordinatorResponseV5} decodes; {@see GroupCoordinatorResponse} decodes the
 * version 6 of Kafka 3.9, whose layout is the same one once more.
 *
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v6)"
 */
final class GroupCoordinatorResponseV4 extends GroupCoordinatorResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
