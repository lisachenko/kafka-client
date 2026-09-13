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
 * JoinGroup answer of version 6 (Kafka 2.4, KIP-482): the flexible frame without the two fields of KIP-559
 *
 * Version 7 (Kafka 2.5) put a nullable `protocol_type` in front of the `protocol_name` and made the name itself
 * nullable, so this is the last version whose {@see self::$groupProtocol} is always a string - the empty one of
 * `GroupCoordinator.NoProtocol` when the answer carries an error.
 *
 * @see docs/protocol/2.8.md, section "The protocol type and name of KIP-559 (Kafka 2.5)"
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v7)"
 */
final class JoinGroupResponseV6 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
