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
 * AlterClientQuotas request, version 0 - the plain frame Kafka 2.6 added (ApiKey 49)
 *
 * Kafka **2.8** added the version 1, which is this very scheme in the compact encoding of KIP-482 and adds no
 * field; this class keeps the frame a 2.6 or 2.7 broker serves, with the request header v1 instead of the v2.
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0 and v1)"
 */
class AlterClientQuotasRequestV0 extends AlterClientQuotasRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
