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
 * GroupCoordinator (FindCoordinator) response, version 1: the answer of version 2 as well
 *
 * <pre>
 *   FindCoordinator Response (Version: 1 and 2) => throttle_time_ms error_code error_message coordinator
 * </pre>
 *
 * @see docs/protocol/2.8.md, sections "GroupCoordinator API (key 10, v0 to v3)" and "Quotas and throttle time"
 */
final class GroupCoordinatorResponseV1 extends GroupCoordinatorResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
