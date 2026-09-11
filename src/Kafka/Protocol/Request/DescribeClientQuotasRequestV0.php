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
 * DescribeClientQuotas request, version 0 - the plain frame of Kafka 2.6 (ApiKey 48)
 *
 * The version 1 of Kafka 2.8 added no field and only turned the encoding compact (KIP-482), so this class is the
 * same scheme written plainly, with the request header v1 instead of the v2.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0 and v1)"
 */
class DescribeClientQuotasRequestV0 extends DescribeClientQuotasRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
