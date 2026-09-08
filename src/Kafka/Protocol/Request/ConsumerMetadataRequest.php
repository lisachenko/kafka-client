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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;

/**
 * The offsets for a given consumer group are maintained by a specific broker called the offset coordinator.
 *
 * A consumer needs to issue its offset commit and fetch requests to this specific broker. It can discover the current
 * coordinator by issuing a consumer metadata request (ApiKey 10, v0); this API was renamed to GroupCoordinator only
 * in Kafka 0.9.
 *
 * ConsumerMetadataRequest => ConsumerGroup
 *   ConsumerGroup => string
 */
class ConsumerMetadataRequest extends AbstractRequest
{
    /**
     * @param string $consumerGroup
     */
    public function __construct(/**
     * The consumer group id.
     */
        private $consumerGroup,
        $clientId = '',
        $correlationId = 0
    ) {
        parent::__construct(ApiKeys::CONSUMER_METADATA, $clientId, $correlationId);
    }

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload     = parent::packPayload();
        $groupLength = strlen($this->consumerGroup);

        $payload .= pack("na{$groupLength}", $groupLength, $this->consumerGroup);

        return $payload;
    }
}
