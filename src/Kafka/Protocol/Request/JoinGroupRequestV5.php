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
 * JoinGroup request of version 5 (Kafka 2.3, KIP-345): the last version with the plain encoding
 *
 * Version 6 (Kafka 2.4, KIP-482) added no field: it is this frame written with the **compact** types and a
 * tagged-field section per structure, which {@see JoinGroupRequestV6} sends.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v7)"
 */
final class JoinGroupRequestV5 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
