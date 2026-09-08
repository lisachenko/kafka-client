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
 */

namespace Protocol\Kafka\Protocol;

/**
 * Numeric codes that the ApiKey in the request can take, as of Kafka 0.8.2.2.
 *
 * The list mirrors kafka/api/RequestKeys.scala @ 0.8.2.2 and stops at key 10: keys 11 and 12 are parsed but not
 * served by a 0.8.2.2 broker ("Unknown api code"), and everything from key 13 upwards does not exist in the 0.8
 * line at all - the broker closes the connection on it.
 */
class ApiKeys
{
    /**
     * The following are the numeric codes that the ApiKey in the request can take for each of the below request types.
     *
     * The names are those of the later protocol lines; key 10 is called ConsumerMetadata in Kafka 0.8.2
     * (kafka/api/RequestKeys.scala) and was renamed to GroupCoordinator in 0.9 without a wire format change.
     */
    public const PRODUCE             = 0;
    public const FETCH               = 1;
    public const OFFSETS             = 2;
    public const METADATA            = 3;
    public const LEADER_AND_ISR      = 4;
    public const STOP_REPLICA        = 5;
    public const UPDATE_METADATA     = 6;
    public const CONTROLLED_SHUTDOWN = 7;
    public const OFFSET_COMMIT       = 8;
    public const OFFSET_FETCH        = 9;
    public const GROUP_COORDINATOR   = 10;
}
