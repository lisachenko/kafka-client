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
 * Metadata response of version 9 (key 3)
 *
 * The first **flexible** answer of the api (Kafka 2.4, KIP-482), with the topic entries of version 8: no
 * `topic_id`, which version 10 (KIP-516) added, and the `cluster_authorized_operations` that version 11
 * (KIP-700) took out again, see {@see MetadataResponse}.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v11)" and "Topic ids (v10, KIP-516)"
 */
final class MetadataResponseV9 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
