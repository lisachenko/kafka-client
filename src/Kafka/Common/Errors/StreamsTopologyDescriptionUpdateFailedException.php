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

namespace Protocol\Kafka\Common\Errors;

use Exception;

/**
 * The broker could not process the topology description update; see the error message for details.
 *
 * Error code 135, Kafka 4.4: KIP-1331: the top-level code of a StreamsGroupTopologyDescriptionUpdate (93) answer whose description the topology description plugin of the broker could not store.
 */
class StreamsTopologyDescriptionUpdateFailedException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED, $previous);
    }
}
