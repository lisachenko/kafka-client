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
 * AlterClientQuotas response, version 0 - the plain frame Kafka 2.6 added (key 49)
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0 and v1)"
 */
class AlterClientQuotasResponseV0 extends AlterClientQuotasResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
