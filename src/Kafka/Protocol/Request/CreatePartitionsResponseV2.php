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
 * CreatePartitions answer of version 2, the frame of version 3 with a lower version field
 *
 * The version 3 of KIP-599 adds no field; it adds the error code 89 `ThrottlingQuotaExceeded` a topic entry may
 * carry when the broker refuses the request instead of queueing it.
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 to v3)"
 */
final class CreatePartitionsResponseV2 extends CreatePartitionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
