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
 * GroupCoordinator, version 0: the group lookup of Kafka 0.8.2, without a coordinator type (key 10)
 *
 * <pre>
 *   GroupCoordinator Request (Version: 0) => group_id
 *     group_id => STRING
 * </pre>
 *
 * The `coordinator_type` that version 1 appended does not exist here, so this class only lowers the version
 * constant that {@see GroupCoordinatorRequest::getScheme()} follows. A version 0 request is always a group lookup -
 * a transactional id can only be asked for with version 1 - and the answer to it carries neither the throttle time
 * nor the error message of version 1, see {@see GroupCoordinatorResponseV0}.
 *
 * @see docs/protocol/2.8.md, section "GroupCoordinator API (key 10, v0 to v2)"
 */
final class GroupCoordinatorRequestV0 extends GroupCoordinatorRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
