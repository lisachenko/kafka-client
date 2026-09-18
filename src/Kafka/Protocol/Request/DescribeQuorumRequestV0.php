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
 * DescribeQuorum request of version 0 (Kafka 2.8, KIP-595): the same frame one api version lower
 *
 * "Version 1 adds additional fields in the response. The request is unchanged (KIP-836)" -
 * `DescribeQuorumRequest.json` @ 3.3.2 - so this class sends byte for byte what
 * {@see DescribeQuorumRequest} sends, and what it buys is in the answer: a request of this version is answered
 * with {@see DescribeQuorumResponseV0}, whose replica states carry the log end offset alone and neither
 * timestamp.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 and v1)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
final class DescribeQuorumRequestV0 extends DescribeQuorumRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
