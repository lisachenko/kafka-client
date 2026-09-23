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
 * One described group of a ConsumerGroupDescribe answer (key 69, Kafka 3.7, KIP-848)
 *
 * <pre>
 *   DescribedGroup => error_code error_message group_id group_state group_epoch assignment_epoch assignor_name
 *                     [members] authorized_operations
 *     error_code            => INT16
 *     error_message         => COMPACT_NULLABLE_STRING
 *     group_id              => COMPACT_STRING
 *     group_state           => COMPACT_STRING
 *     group_epoch           => INT32
 *     assignment_epoch      => INT32
 *     assignor_name         => COMPACT_STRING
 *     members               => member_id … assignment target_assignment
 *     authorized_operations => INT32
 * </pre>
 *
 * The `DescribedGroup` structure of `ConsumerGroupDescribeResponse.json` @ 3.9.2. Like the classic DescribeGroups
 * (key 15) the api reports the error of every group **inside** its entry and keeps the frame itself error-free,
 * and unlike it a group the coordinator does not have is a **69** `GroupIdNotFound` here rather than the
 * `Dead` state of an entry with the code 0.
 *
 * The three epochs are what the new protocol replaced the generation with:
 *
 * * `group_epoch` is bumped whenever the *input* of the assignment changes - a member joins or leaves, a
 *   subscription changes, a topic gains partitions;
 * * `assignment_epoch` is the group epoch the current **target** assignment was computed for; while it lags
 *   behind the group epoch, the coordinator is still computing;
 * * the `member_epoch` of every member is the group epoch that member has caught up with. A settled group has
 *   all three equal.
 *
 * The **states** of `GroupState` @ 3.9.2 are `Empty`, `Assigning`, `Reconciling`, `Stable` and `Dead`, which are
 * not the states of a classic group ({@see DescribeGroupResponseMetadata}): there is no `PreparingRebalance` and
 * no `CompletingRebalance`, because the new protocol has no stop-the-world rebalance to prepare or complete.
 *
 * @see \Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeResponse
 * @see docs/protocol/3.9.md, section "ConsumerGroupDescribe API (key 69, v0)"
 */
class ConsumerGroupDescribedGroup implements BinarySchemaInterface
{
    /**
     * A group that has no member and no assignment: `ConsumerGroup.ConsumerGroupState.EMPTY` @ 3.9.2
     */
    public const string STATE_EMPTY = 'Empty';

    /**
     * The coordinator is computing a new target assignment for the current group epoch
     */
    public const string STATE_ASSIGNING = 'Assigning';

    /**
     * The target assignment is there and the members are still catching up with it
     */
    public const string STATE_RECONCILING = 'Reconciling';

    /**
     * Every member owns exactly what the target assignment says
     */
    public const string STATE_STABLE = 'Stable';

    /**
     * The group is gone, which is how a deleted group is described while its tombstone is written
     */
    public const string STATE_DEAD = 'Dead';

    /**
     * Error of this group, or 0 when it was described
     */
    public int $errorCode = 0;

    /**
     * Message of that error, null when there was none
     */
    public ?string $errorMessage = null;

    /**
     * Name of the group
     */
    public string $groupId = '';

    /**
     * State of the group, one of the `STATE_*` constants of this class, the empty string for a refused entry
     */
    public string $groupState = '';

    /**
     * Epoch of the group, bumped whenever the input of the assignment changed
     */
    public int $groupEpoch = 0;

    /**
     * Group epoch the current target assignment was computed for
     */
    public int $assignmentEpoch = 0;

    /**
     * Name of the server-side assignor the group settled on, e.g. `uniform` or `range`
     */
    public string $assignorName = '';

    /**
     * Members of the group, indexed by their member id
     *
     * @var array<string, ConsumerGroupDescribeMember>
     */
    public array $members = [];

    /**
     * Operations the principal of this connection may perform on the group, as the bit field of KIP-430
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
            'members'              => ['memberId' => ConsumerGroupDescribeMember::class],
            'authorizedOperations' => BinarySchema::TYPE_INT32,
        ];
    }
}
