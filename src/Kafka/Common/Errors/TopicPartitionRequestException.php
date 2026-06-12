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

/**
 * Class TopicPartitionRequestException
 */
class TopicPartitionRequestException extends \RuntimeException implements ServerExceptionInterface
{
    /**
     * Exceptions during this query [topic][partition] => exception
     *
     * @var Exception[][]
     */
    protected array $exceptions;

    /**
     * TopicPartitionRequestException constructor.
     *
     * @param array         $partialResult Partial result received from broker
     * @param Exception[][] $exceptions    List of nested exceptions
     */
    public function __construct(/**
     * Partial result ready to use [topic][partition] => partition result
     */
        private readonly array $partialResult,
        array $exceptions
    ) {
        $message = '';
        foreach ($exceptions as $topic => $partitions) {
            foreach ($partitions as $partitionId => $exception) {
                $message .= sprintf("%s:%s %s\n", $topic, $partitionId, $exception->getMessage());
            }
        }
        parent::__construct('Request completed with errors: ' . $message);
        $this->exceptions    = $exceptions;
    }

    /**
     * Return partial result from request
     *
     * @return array [topic][partition] => partition result
     */
    public function getPartialResult()
    {
        return $this->partialResult;
    }

    /**
     * Return array of occurred exceptions
     *
     * @return Exception[][] [topic][partition] => exception
     */
    public function getExceptions()
    {
        return $this->exceptions;
    }
}
