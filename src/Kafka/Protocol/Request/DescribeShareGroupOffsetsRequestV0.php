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
 * DescribeShareGroupOffsets request of version 0 (Kafka 4.1, KIP-932)
 *
 * The version 1 of Kafka 4.2 (KIP-1226) changed the answer only: the request of both versions is the same frame,
 * so this class only lowers the version constant. A node that answers it leaves the `lag` out of every partition
 * ({@see DescribeShareGroupOffsetsResponseV0}).
 *
 * @see docs/protocol/4.3.md, section "The share-partition lag of KIP-1226 (v1)"
 */
final class DescribeShareGroupOffsetsRequestV0 extends DescribeShareGroupOffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
