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
 * DeleteGroups answer of version 2, the flexible frame before the error message of KIP-1331
 *
 * The version 3 of Kafka 4.4 appended `error_message` to every group entry ({@see DeleteGroupsResponse}). This class
 * keeps the entry without it - its groups are read through
 * {@see \Protocol\Kafka\Protocol\Data\DeleteGroupsResponseGroupV2} - and {@see DeleteGroupsResponseV1} keeps the
 * plain encoding of Kafka 2.0.
 *
 * @see docs/protocol/overview.md, section "DeleteGroups API (key 42, v0 to v3)"
 */
final class DeleteGroupsResponseV2 extends DeleteGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
