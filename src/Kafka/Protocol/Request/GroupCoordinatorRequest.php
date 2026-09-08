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
 * The offsets for a given consumer group are maintained by a specific broker called the group coordinator. i.e., a
 * consumer needs to issue its offset commit and fetch requests to this specific broker.
 *
 * It can discover the current coordinator by issuing a group coordinator request.
 *
 * The API is called ConsumerMetadataRequest in Kafka 0.8.2 (api key 10, v0, kafka/api/ConsumerMetadataRequest.scala);
 * it was renamed to GroupCoordinator in 0.9 without a change to the wire format.
 *
 * <pre>
 *   GroupCoordinatorRequest => ConsumerGroup
 *     ConsumerGroup => string
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "GroupCoordinator API (key 10, v0)"
 */
class GroupCoordinatorRequest extends AbstractRequest
{
    public function __construct(
        /**
         * The consumer group id.
         */
        protected readonly string $consumerGroup,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(ApiKeys::GROUP_COORDINATOR, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'consumerGroup' => BinarySchema::TYPE_STRING,
        ];
    }
}
