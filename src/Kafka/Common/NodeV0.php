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

namespace Protocol\Kafka\Common;

/**
 * Broker entry of a Metadata response of version 0, i.e. the one without a `Rack`
 *
 * <pre>
 *   Broker => NodeId Host Port
 * </pre>
 *
 * `METADATA_BROKER_V0` in `Protocol.java` @ 0.10.2.2. The class exists only to lower the version constant that
 * {@see Node::getScheme()} follows; the `rack` property of the parent stays null for it. Reading a version 0
 * answer with the version 1 entry would take the first bytes of the topic array for the length of a rack string.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v7)"
 */
final class NodeV0 extends Node
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
