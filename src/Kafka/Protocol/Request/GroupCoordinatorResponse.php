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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;

/**
 * Group coordinator response
 *
 * Called ConsumerMetadataResponse in Kafka 0.8.2 (api key 10, v0); the wire format below is unchanged in 0.9.
 *
 * <pre>
 *   GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
 *     ErrorCode       => int16
 *     CoordinatorId   => int32
 *     CoordinatorHost => string
 *     CoordinatorPort => int32
 * </pre>
 *
 * While the broker is still creating the internal `__consumer_offsets` topic the answer is the error code 15
 * (GroupCoordinatorNotAvailable) with the coordinator `-1:"":-1`, so the lookup is worth retrying.
 *
 * @see docs/protocol/0.8.2.md, section "GroupCoordinator API (key 10, v0)"
 */
class GroupCoordinatorResponse extends AbstractResponse
{
    /**
     * Error code.
     */
    public int $errorCode;

    /**
     * Host and port information for the coordinator for a consumer group.
     */
    public GroupCoordinatorResponseMetadata $coordinator;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'   => BinarySchema::TYPE_INT16,
            'coordinator' => GroupCoordinatorResponseMetadata::class,
        ];
    }
}
