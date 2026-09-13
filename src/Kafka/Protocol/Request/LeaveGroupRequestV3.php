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
 * LeaveGroup request of version 3 (Kafka 2.4, KIP-345): the batch, plainly encoded
 *
 * Version 4 (Kafka 2.4, KIP-482) added no field: it is the batch of version 3 written with the **compact**
 * types and a tagged-field section per structure, which {@see LeaveGroupRequest} sends. The two versions
 * are the same release: KIP-345 gave the api its batch and KIP-482 the flexible encoding.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "LeaveGroup API (key 13, v0 to v4)"
 */
final class LeaveGroupRequestV3 extends LeaveGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
