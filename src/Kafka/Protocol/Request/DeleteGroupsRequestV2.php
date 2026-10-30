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
 * DeleteGroups request of version 2, the first flexible one (Kafka 2.4, KIP-482)
 *
 * Kafka 4.4 added the version 3 and changed no field of the request - "The request body shape is unchanged at
 * version 3" of `DeleteGroupsRequest.json` @ 4.4.0 - so this class only lowers the version constant: the version 2
 * is what a broker below Kafka 4.4 is asked with, and its answer carries no error message per group
 * ({@see DeleteGroupsResponseV2}).
 *
 * @see docs/protocol/4.4.md, section "DeleteGroups API (key 42, v0 to v3)"
 */
final class DeleteGroupsRequestV2 extends DeleteGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
