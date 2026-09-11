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
 * Requested credential would not meet criteria for acceptability.
 *
 * Error code 93, Kafka 2.7 (KIP-554): an AlterUserScramCredentials (51) upsert carries an unusable credential: an
 * empty user name, an unknown mechanism, an iteration count below the minimum of the mechanism (4096 for SCRAM-SHA-256
 * and SCRAM-SHA-512) or a salt or salted password of the wrong length.
 */
class UnacceptableCredentialException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNACCEPTABLE_CREDENTIAL, $previous);
    }
}
