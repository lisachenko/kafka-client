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
 * DeleteGroups request of version 1 (Kafka 2.0, KIP-219): the group array, plainly encoded
 *
 * Version 2 (Kafka 2.4, KIP-482) added no field: it is this frame with a **compact** group array and a
 * tagged-field section, which {@see DeleteGroupsRequest} sends.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "DeleteGroups API (key 42, v0 to v2)"
 */
final class DeleteGroupsRequestV1 extends DeleteGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
