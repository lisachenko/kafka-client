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
 * SyncGroup request of version 3 (Kafka 2.3, KIP-345): the last version with the plain encoding
 *
 * Version 4 (Kafka 2.4, KIP-482) added no field: it is this frame written with the **compact** types and a
 * tagged-field section per structure, which {@see SyncGroupRequest} sends.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "SyncGroup API (key 14, v0 to v4)"
 */
final class SyncGroupRequestV3 extends SyncGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
