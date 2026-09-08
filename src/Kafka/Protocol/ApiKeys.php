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
 * Numeric codes that the ApiKey in the request can take, as of Kafka 0.9.0.1.
 *
 * The list mirrors kafka/api/RequestKeys.scala @ 0.9.0.1 and stops at key 16: SaslHandshake (17) and ApiVersions (18)
 * arrived with Kafka 0.10, and everything above them is later still. A 0.9.0.1 broker does not answer a request with
 * an api key it does not know and does not close the connection either - it logs "Processor got uncaught exception"
 * and drops the request silently, so a client that sends one waits for its own timeout.
 */
class ApiKeys
{
    /**
     * The following are the numeric codes that the ApiKey in the request can take for each of the below request types.
     *
     * The names are those of the later protocol lines; key 10 is called ConsumerMetadata in Kafka 0.8.2 and was
     * renamed to GroupCoordinator in 0.9 (kafka/api/RequestKeys.scala @ 0.9.0.1) without a wire format change.
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
    public const JOIN_GROUP          = 11;
    public const HEARTBEAT           = 12;
    public const LEAVE_GROUP         = 13;
    public const SYNC_GROUP          = 14;
    public const DESCRIBE_GROUPS     = 15;
    public const LIST_GROUPS         = 16;
}
