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
 * DeleteGroups response, version 1: one error code per group, plainly encoded
 *
 * Version 2 (Kafka 2.4, KIP-482) is the same answer in the flexible encoding, see {@see DeleteGroupsResponse}.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "DeleteGroups API (key 42, v0 to v2)"
 */
final class DeleteGroupsResponseV1 extends DeleteGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
