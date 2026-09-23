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
 * FindCoordinator answer of version 3 (Kafka 2.4, KIP-482): one coordinator at the top level, flexibly encoded
 *
 * Version 4 (Kafka 3.0, KIP-699) moved the error code, the error message and the three fields of the coordinator
 * into a `coordinators` array, one entry per key of the request; this version answers the one key its request
 * named. {@see GroupCoordinatorResponse} decodes the batched answer.
 *
 * @see docs/protocol/3.9.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v6)"
 */
final class GroupCoordinatorResponseV3 extends GroupCoordinatorResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
