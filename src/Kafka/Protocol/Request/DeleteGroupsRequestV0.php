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
 * DeleteGroups request of version 0 (Kafka 1.1, KIP-229), the frame of version 1 with a lower version field
 *
 * <pre>
 *   DeleteGroups Request (Version: 0 and 1) => [groups]
 * </pre>
 *
 * Version 1 (KIP-219, Kafka 2.0) left the group array alone - `DeleteGroupsRequest.json` @ 2.8.2 has the single
 * field `GroupsNames` at `0+` - so this class puts the very same bytes on the wire as {@see DeleteGroupsRequest}
 * and reads its answer with {@see DeleteGroupsResponseV0}.
 *
 * @see docs/protocol/2.8.md, sections "DeleteGroups API (key 42, v0 and v1)" and "Quotas and throttle time"
 */
final class DeleteGroupsRequestV0 extends DeleteGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
