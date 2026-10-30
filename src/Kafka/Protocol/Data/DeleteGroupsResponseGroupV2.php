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

namespace Protocol\Kafka\Protocol\Data;

/**
 * The result of one group of a DeleteGroups answer up to version 2, i.e. the group id and its code alone
 *
 * Kafka 4.4 appended `error_message` to the group entry ({@see DeleteGroupsResponseGroup}), behind the code of
 * KIP-229; this class is the entry of the versions 0 to 2, which ends with the code and leaves
 * {@see DeleteGroupsResponseGroup::$errorMessage} at the null of the default.
 *
 * @see docs/protocol/overview.md, section "DeleteGroups API (key 42, v0 to v3)"
 */
final class DeleteGroupsResponseGroupV2 extends DeleteGroupsResponseGroup
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
