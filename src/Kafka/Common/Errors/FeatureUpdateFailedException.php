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
 * Unable to update finalized features due to an unexpected server error.
 *
 * Error code 96, Kafka 2.7 (KIP-584, UpdateFeatures): the controller could not write the finalized features of an
 * UpdateFeatures (57) to ZooKeeper.
 */
class FeatureUpdateFailedException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::FEATURE_UPDATE_FAILED, $previous);
    }
}
