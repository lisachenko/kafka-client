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
 * Metadata response of version 8 (key 3)
 *
 * The **plain** encoding of the frame version 9 answers: the throttle time, the brokers, the cluster id, the
 * controller id, the topics with their partitions and the two `authorized_operations` bitfields of KIP-430.
 * Version 9 (Kafka 2.4) writes the same fields with compact strings and arrays, a tag buffer behind the
 * correlation id of the header and a tagged-field section at the end of every structure, see
 * {@see MetadataResponse}.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v9)" and
 *      "Flexible versions in the engine (KIP-482)"
 */
final class MetadataResponseV8 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
