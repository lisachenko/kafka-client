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
 * GroupCoordinator response, version 0: the error code and the coordinator, and nothing else (key 10)
 *
 * <pre>
 *   GroupCoordinator Response (Version: 0) => error_code coordinator
 *     error_code  => INT16
 *     coordinator => node_id host port
 * </pre>
 *
 * Neither the `throttle_time_ms` that opens a version 1 answer nor the `error_message` that follows the error code
 * there exists in this layout, so this class only lowers the version constant that
 * {@see GroupCoordinatorResponse::getScheme()} follows; reading a version 0 answer with the version 1 class would
 * take the four bytes of the node id for a throttle time. {@see GroupCoordinatorResponse::$throttleTimeMs} stays 0
 * and {@see GroupCoordinatorResponse::$errorMessage} stays null for an instance of this class.
 *
 * @see docs/protocol/2.8.md, section "GroupCoordinator API (key 10, v0 and v1)"
 */
final class GroupCoordinatorResponseV0 extends GroupCoordinatorResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
