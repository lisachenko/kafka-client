<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Common\Errors;

use Exception;
use RuntimeException;

/**
 * Consumer did not receive any subscription from the leader
 */
class EmptyAssignmentException extends RuntimeException implements ClientExceptionInterface
{
    /**
     * EmptyAssignmentException constructor.
     *
     * @param string[]       $requestedTopics List of requested topic subscription
     * @param Exception|null $previous        Previous exception if there was one
     */
    public function __construct(/**
     * List of requested topic subscription
     */
        private readonly array $requestedTopics,
        ?Exception $previous = null
    ) {
        parent::__construct('Consumer did not receive any subscription from the leader.', 0, $previous);
    }

    /**
     * Return list of requested topic subscription
     *
     * @return string[]
     */
    public function getRequestedTopics()
    {
        return $this->requestedTopics;
    }
}
