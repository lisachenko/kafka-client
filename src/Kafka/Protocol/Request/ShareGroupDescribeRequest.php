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
 * ShareGroupDescribe, version 1: describes share groups of KIP-932 (ApiKey 77, Kafka 4.1)
 *
 * <pre>
 *   ShareGroupDescribe Request (Version: 1) => [group_ids] include_authorized_operations
 * </pre>
 *
 * The request of ConsumerGroupDescribe ({@see ConsumerGroupDescribeRequest}) for share groups; `validVersions` is `1`
 * @ 4.1.0, the version 0 of the early access of Kafka 4.0 is gone. A group of another type is the **69** of its
 * entry. `AdminClient::describeShareGroups()` sends it.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupDescribe API (key 77, v1)"
 */
class ShareGroupDescribeRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::SHARE_GROUP_DESCRIBE;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * The api is flexible from its first version
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
        return parent::getScheme() + [
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
