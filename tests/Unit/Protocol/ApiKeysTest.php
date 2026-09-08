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

namespace Protocol\Kafka\Tests\Unit\Protocol;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Protocol\ApiKeys;
use ReflectionClass;

/**
 * Verifies that the api keys of this branch are exactly the ones of Kafka 0.9.0.1.
 *
 * The list mirrors kafka/api/RequestKeys.scala @ 0.9.0.1, which ends at key 16. The names are those of the later
 * protocol lines (branch main): key 10 is GroupCoordinator here and ConsumerMetadata in the 0.8.2 sources.
 *
 * @see \Protocol\Kafka\Tests\Integration\ApiVersionProbeTest for the same list verified against a real broker
 */
#[CoversClass(ApiKeys::class)]
final class ApiKeysTest extends TestCase
{
    /**
     * Every api key of Kafka 0.9.0.1, in the order of RequestKeys.scala
     *
     * @var array<string, int>
     */
    private const array KEYS_OF_KAFKA_0_9_0_1 = [
        'PRODUCE'             => 0,
        'FETCH'               => 1,
        'OFFSETS'             => 2,
        'METADATA'            => 3,
        'LEADER_AND_ISR'      => 4,
        'STOP_REPLICA'        => 5,
        'UPDATE_METADATA'     => 6,
        'CONTROLLED_SHUTDOWN' => 7,
        'OFFSET_COMMIT'       => 8,
        'OFFSET_FETCH'        => 9,
        'GROUP_COORDINATOR'   => 10,
        'JOIN_GROUP'          => 11,
        'HEARTBEAT'           => 12,
        'LEAVE_GROUP'         => 13,
        'SYNC_GROUP'          => 14,
        'DESCRIBE_GROUPS'     => 15,
        'LIST_GROUPS'         => 16,
    ];

    public function testTheApiKeysAreExactlyTheOnesOfKafka0901(): void
    {
        self::assertSame(
            self::KEYS_OF_KAFKA_0_9_0_1,
            new ReflectionClass(ApiKeys::class)->getConstants(),
            'The branch declares an api key that Kafka 0.9.0.1 does not have, or misses one that it has'
        );
    }

    public function testTheGroupMembershipKeysOfKafka09AreDeclared(): void
    {
        self::assertSame(11, ApiKeys::JOIN_GROUP);
        self::assertSame(12, ApiKeys::HEARTBEAT);
        self::assertSame(13, ApiKeys::LEAVE_GROUP);
        self::assertSame(14, ApiKeys::SYNC_GROUP);
        self::assertSame(15, ApiKeys::DESCRIBE_GROUPS);
        self::assertSame(16, ApiKeys::LIST_GROUPS);
    }

    /**
     * SaslHandshake (17) and ApiVersions (18) arrived with Kafka 0.10 and must not appear on this branch
     */
    public function testNoApiKeyOfALaterKafkaIsDeclared(): void
    {
        $keys = new ReflectionClass(ApiKeys::class)->getConstants();

        self::assertSame(range(0, 16), array_values($keys));
        self::assertNotContains('SASL_HANDSHAKE', array_keys($keys));
        self::assertNotContains('API_VERSIONS', array_keys($keys));
    }
}
