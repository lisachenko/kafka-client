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

use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One described group of a ShareGroupDescribe answer (key 77, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DescribedGroup => error_code error_message group_id group_state group_epoch assignment_epoch assignor_name
 *                     [members] authorized_operations
 * </pre>
 *
 * The `DescribedGroup` structure of `ShareGroupDescribeResponse.json` @ 4.1.0 - the very layout of the consumer
 * protocol's ({@see ConsumerGroupDescribedGroup}). Every group carries its own error: the **69** `GroupIdNotFound` for
 * a group that does not exist or is not a share group, the **30** for a group the principal may not describe.
 *
 * The states of a share group are `Empty`, `Stable` and `Dead` (`ShareGroup.ShareGroupState` @ 4.1.0): a share group
 * never reconciles, so it has no `Assigning` and no `Reconciling`.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupDescribe API (key 77, v1)"
 */
final class ShareGroupDescribedGroup implements BinarySchemaInterface
{
    /**
     * A group without members
     */
    public const string STATE_EMPTY = 'Empty';

    /**
     * A group with members
     */
    public const string STATE_STABLE = 'Stable';

    /**
     * A group that is being deleted
     */
    public const string STATE_DEAD = 'Dead';

    /**
     * Error code of the group, 0 when it was described
     */
    public int $errorCode = 0;

    /**
     * Error message of the group, null when it was described
     */
    public ?string $errorMessage = null;

    /**
     * Name of the group
     */
    public string $groupId = '';

    /**
     * State of the group, one of the `STATE_*` constants, or the empty string of a refused entry
     */
    public string $groupState = '';

    /**
     * Epoch of the group, bumped by every change of its membership or subscriptions
     */
    public int $groupEpoch = 0;

    /**
     * Group epoch the current assignment was computed for
     */
    public int $assignmentEpoch = 0;

    /**
     * Name of the assignor of the group, `simple` for a share group
     */
    public string $assignorName = '';

    /**
     * Members of the group, indexed by the member id
     *
     * @var array<string, ShareGroupDescribeMember>
     */
    public array $members = [];

    /**
     * Operations the principal may perform on the group, {@see AclOperation::NOT_REQUESTED} when not asked for
     */
    public int $authorizedOperations = AclOperation::NOT_REQUESTED;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'            => BinarySchema::TYPE_INT16,
            'errorMessage'         => BinarySchema::TYPE_NULLABLE_STRING,
            'groupId'              => BinarySchema::TYPE_STRING,
            'groupState'           => BinarySchema::TYPE_STRING,
            'groupEpoch'           => BinarySchema::TYPE_INT32,
            'assignmentEpoch'      => BinarySchema::TYPE_INT32,
            'assignorName'         => BinarySchema::TYPE_STRING,
            'members'              => ['memberId' => ShareGroupDescribeMember::class],
            'authorizedOperations' => BinarySchema::TYPE_INT32,
        ];
    }
}
