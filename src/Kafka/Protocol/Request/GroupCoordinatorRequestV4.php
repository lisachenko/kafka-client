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
 * FindCoordinator request of version 4 (Kafka 3.0, KIP-699): the batched lookup before KIP-890
 *
 * Version 5 (Kafka 3.8, KIP-890) added no field to either half of the api - *"Version 5 adds support for new error
 * code TRANSACTION_ABORTABLE (KIP-890)"* - so this frame is the version 5 frame with the number 4 in its header,
 * and it is the highest version a broker below Kafka 3.8 serves. {@see GroupCoordinatorRequest} sends the version 5.
 *
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v5)"
 */
final class GroupCoordinatorRequestV4 extends GroupCoordinatorRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
