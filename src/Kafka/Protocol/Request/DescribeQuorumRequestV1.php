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
 * DescribeQuorum request of version 1 (Kafka 3.3, KIP-836): the same frame one api version lower
 *
 * "Version 2 adds additional fields in the response. The request is unchanged (KIP-853)" -
 * `DescribeQuorumRequest.json` @ 3.9.2 - so this class sends byte for byte what {@see DescribeQuorumRequest}
 * sends, and what it buys is in the answer: a request of this version is answered with
 * {@see DescribeQuorumResponseV1}, which has no top-level nodes, no error message and no directory id in a
 * replica state.
 *
 * @see docs/protocol/4.3.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
final class DescribeQuorumRequestV1 extends DescribeQuorumRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
