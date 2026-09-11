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
 * DeleteGroups response, version 0: the throttle time and one result per group, the answer of version 1 as well
 *
 * <pre>
 *   DeleteGroups Response (Version: 0 and 1) => throttle_time_ms [group_error_codes]
 * </pre>
 *
 * @see docs/protocol/2.8.md, sections "DeleteGroups API (key 42, v0 to v2)" and "Quotas and throttle time"
 */
final class DeleteGroupsResponseV0 extends DeleteGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
