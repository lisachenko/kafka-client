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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestPartitionV0;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochResponsePartition;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequest;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequestV1;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequestV2;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponse;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponseV1;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponseV2;

/**
 * The frames of the OffsetForLeaderEpoch api (key 23), version by version.
 *
 * The api was broker-to-broker until Kafka 2.1 gave it a `current_leader_epoch` (KIP-320) and made it the
 * position validation of a consumer; Kafka 2.3 added a `replica_id` at the head of the request (KIP-392), because
 * a consumer may now validate against a follower.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v3)"
 */
#[CoversClass(OffsetForLeaderEpochRequest::class)]
#[CoversClass(OffsetForLeaderEpochResponse::class)]
#[CoversClass(OffsetForLeaderEpochRequestPartition::class)]
final class OffsetForLeaderEpochApiTest extends TestCase
{
    public function testVersionThreeWritesTheReplicaIdInFrontOfTheTopics(): void
    {
        $consumer = bin2hex((string) new OffsetForLeaderEpochRequest(
            ['topic' => [0 => [7, 9]]],
            'test',
            3,
            OffsetForLeaderEpochRequest::CONSUMER_REPLICA_ID
        ));
        $debug = bin2hex((string) new OffsetForLeaderEpochRequest(['topic' => [0 => [7, 9]]], 'test', 3));

        //   Size, ApiKey 0017, ApiVersion 0003, CorrelationId, ClientId "test", then the replica id
        self::assertSame('00170003' . '00000003' . '0004' . '74657374' . 'ffffffff', substr($consumer, 8, 36));
        self::assertSame('fffffffe', substr($debug, 8 + 28, 8), '-2, the default of the field, is a debug client');
        self::assertSame(-1, OffsetForLeaderEpochRequest::CONSUMER_REPLICA_ID);
        self::assertSame(-2, OffsetForLeaderEpochRequest::DEBUG_REPLICA_ID);

        // The body behind the replica id is the body of version 2: the epoch of the position and the epoch the
        // client believes the partition is led with, in that order
        self::assertSame(
            substr(bin2hex((string) new OffsetForLeaderEpochRequestV2(['topic' => [0 => [7, 9]]], 'test', 3)), 44),
            substr($consumer, 44 + 8),
            'the body behind the replica id is the body of version 2'
        );
        self::assertSame(3, OffsetForLeaderEpochRequest::VERSION);
        self::assertSame(3, OffsetForLeaderEpochResponse::VERSION);
        self::assertSame(2, OffsetForLeaderEpochRequestV2::VERSION);
    }

    public function testEveryVersionOfTheRequestWritesExactlyTheFieldsItHas(): void
    {
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'topics'],
            array_keys(OffsetForLeaderEpochRequest::getScheme())
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'topics'],
            array_keys(OffsetForLeaderEpochRequestV2::getScheme()),
            'the replica id of KIP-392 arrived with version 3'
        );
        self::assertSame(
            ['partition', 'currentLeaderEpoch', 'leaderEpoch'],
            array_keys(OffsetForLeaderEpochRequestPartition::getScheme())
        );
        self::assertSame(
            ['partition', 'leaderEpoch'],
            array_keys(OffsetForLeaderEpochRequestPartitionV0::getScheme()),
            'the current leader epoch of KIP-320 arrived with version 2'
        );
    }

    public function testTheAnswerOfVersionThreeIsTheAnswerOfVersionTwo(): void
    {
        // `OffsetForLeaderEpochResponse.json` @ 2.8.2: "Version 3 is the same as version 2"
        self::assertSame(OffsetForLeaderEpochResponseV2::getScheme(), OffsetForLeaderEpochResponse::getScheme());
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'topics'],
            array_keys(OffsetForLeaderEpochResponse::getScheme())
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'topics'],
            array_keys(OffsetForLeaderEpochResponseV1::getScheme()),
            'the throttle time arrived with version 2, at the head of the frame'
        );

        // The partition entry of the answer puts the error code FIRST, which no other api of this protocol does
        self::assertSame(
            ['errorCode', 'partition', 'leaderEpoch', 'endOffset'],
            array_keys(OffsetForLeaderEpochResponsePartition::getScheme())
        );
        self::assertSame(-1, OffsetForLeaderEpochResponsePartition::UNDEFINED_EPOCH);
        self::assertSame(-1, OffsetForLeaderEpochResponsePartition::UNDEFINED_EPOCH_OFFSET);
        self::assertSame(1, OffsetForLeaderEpochRequestV1::VERSION);
    }
}
