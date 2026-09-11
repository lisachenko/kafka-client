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
 * FindCoordinator request of version 2 (Kafka 2.0, KIP-219): the last version with the plain encoding
 *
 * Version 3 (Kafka 2.4, KIP-482) added no field: it is the version 1 frame - the key and its type - written
 * with the **compact** types and a tagged-field section, which {@see GroupCoordinatorRequest} sends.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "GroupCoordinator API (key 10, v0 to v3)"
 */
final class GroupCoordinatorRequestV2 extends GroupCoordinatorRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
