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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * ConsumerGroupDescribe, version 1: describes groups of the new consumer protocol (ApiKey 69, Kafka 3.7, KIP-848)
 *
 * <pre>
 *   ConsumerGroupDescribe Request (Version: 0 and 1) => [group_ids] include_authorized_operations
 *     group_ids                     => COMPACT_STRING
 *     include_authorized_operations => BOOLEAN
 * </pre>
 *
 * The request is the DescribeGroups (key 15) of the new protocol, and the two apis do not overlap: a **classic**
 * group asked of this one is refused with the **69** `GroupIdNotFound`, and a group of the new protocol asked of
 * DescribeGroups is refused the same way. The answer is what key 15 could not carry -
 * {@see ConsumerGroupDescribeResponse} - and `AdminClient::describeConsumerGroups()` is the method that sends it.
 *
 * The boolean is the `include_authorized_operations` of KIP-430, which every describe api of this protocol has
 * had since Kafka 2.3; it is a field of version 0 here, because the api was born long after that KIP.
 *
 * The request goes to the **coordinator** of every group it names, as every group api does, and a broker that
 * does not coordinate one of them answers that entry - not the frame - with the 16 `NotCoordinatorForGroup`.
 *
 * **Version 1 (Kafka 4.0, KIP-1099) changes the answer only**: `ConsumerGroupDescribeRequest.json` @ 4.0.0 says
 * *"For ConsumerGroupDescribeRequest, version 1 is same as version 0"*, and the answer gives every member its
 * `member_type` ({@see \Protocol\Kafka\Protocol\Data\ConsumerGroupDescribeMember::$memberType}).
 * {@see ConsumerGroupDescribeRequestV0} is the frame of version 0.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupDescribe API (key 69, v0 and v1)"
 */
class ConsumerGroupDescribeRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::CONSUMER_GROUP_DESCRIBE;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * The api is flexible from its first version: it was born after KIP-482 (Kafka 2.4)
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Groups this request asks for
     *
     * @var list<string>
     */
    protected readonly array $groupIds;

    /**
     * @param list<string> $groupIds                    Groups to describe
     * @param bool         $includeAuthorizedOperations Whether the answer reports the operations this client may
     *        perform on each group (KIP-430)
     * @param string       $clientId                    An identifier of the client
     * @param int          $correlationId               A value the broker passes back unmodified
     */
    public function __construct(
        array $groupIds,
        /**
         * Whether the answer names the authorized operations of every group
         */
        protected readonly bool $includeAuthorizedOperations = false,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->groupIds = array_values($groupIds);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'groupIds'                    => [BinarySchema::TYPE_STRING],
            'includeAuthorizedOperations' => BinarySchema::TYPE_BOOLEAN,
        ];
    }

    /**
     * Returns the groups this request asks for
     *
     * @return list<string>
     */
    public function getGroupIds(): array
    {
        return $this->groupIds;
    }
}
