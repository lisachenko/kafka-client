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
 * Verifies that the api keys of this branch are exactly the ones of Kafka 1.1.1.
 *
 * The list mirrors `org.apache.kafka.common.protocol.ApiKeys` @ 1.1.1, which ends at key 42. The names are the
 * ones of the pre-schema `main` branch wherever the concept exists there: key 10 is GroupCoordinator here, although
 * 0.11 renamed the api to FindCoordinator and the 0.8.2 sources called it ConsumerMetadata; the keys 34 to 42 carry
 * the names of the Java client.
 *
 * @see \Protocol\Kafka\Tests\Integration\ApiVersionProbeTest for the same list verified against a real broker
 */
#[CoversClass(ApiKeys::class)]
final class ApiKeysTest extends TestCase
{
    /**
     * Every api key of Kafka 1.1.1, in the order of org.apache.kafka.common.protocol.ApiKeys
     *
     * @var array<string, int>
     */
    private const array KEYS_OF_KAFKA_1_1_1 = [
        'PRODUCE'                 => 0,
        'FETCH'                   => 1,
        'OFFSETS'                 => 2,
        'METADATA'                => 3,
        'LEADER_AND_ISR'          => 4,
        'STOP_REPLICA'            => 5,
        'UPDATE_METADATA'         => 6,
        'CONTROLLED_SHUTDOWN'     => 7,
        'OFFSET_COMMIT'           => 8,
        'OFFSET_FETCH'            => 9,
        'GROUP_COORDINATOR'       => 10,
        'JOIN_GROUP'              => 11,
        'HEARTBEAT'               => 12,
        'LEAVE_GROUP'             => 13,
        'SYNC_GROUP'              => 14,
        'DESCRIBE_GROUPS'         => 15,
        'LIST_GROUPS'             => 16,
        'SASL_HANDSHAKE'          => 17,
        'API_VERSIONS'            => 18,
        'CREATE_TOPICS'           => 19,
        'DELETE_TOPICS'           => 20,
        'DELETE_RECORDS'          => 21,
        'INIT_PRODUCER_ID'        => 22,
        'OFFSET_FOR_LEADER_EPOCH' => 23,
        'ADD_PARTITIONS_TO_TXN'   => 24,
        'ADD_OFFSETS_TO_TXN'      => 25,
        'END_TXN'                 => 26,
        'WRITE_TXN_MARKERS'       => 27,
        'TXN_OFFSET_COMMIT'       => 28,
        'DESCRIBE_ACLS'           => 29,
        'CREATE_ACLS'             => 30,
        'DELETE_ACLS'             => 31,
        'DESCRIBE_CONFIGS'        => 32,
        'ALTER_CONFIGS'           => 33,
        'ALTER_REPLICA_LOG_DIRS'  => 34,
        'DESCRIBE_LOG_DIRS'       => 35,
        'SASL_AUTHENTICATE'       => 36,
        'CREATE_PARTITIONS'       => 37,
        'CREATE_DELEGATION_TOKEN'   => 38,
        'RENEW_DELEGATION_TOKEN'    => 39,
        'EXPIRE_DELEGATION_TOKEN'   => 40,
        'DESCRIBE_DELEGATION_TOKEN' => 41,
        'DELETE_GROUPS'             => 42,
    ];

    public function testTheApiKeysAreExactlyTheOnesOfKafka111(): void
    {
        self::assertSame(
            self::KEYS_OF_KAFKA_1_1_1,
            new ReflectionClass(ApiKeys::class)->getConstants(),
            'The branch declares an api key that Kafka 1.1.1 does not have, or misses one that it has'
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
     * SaslHandshake (17), ApiVersions (18), CreateTopics (19) and DeleteTopics (20) arrived with Kafka 0.10
     */
    public function testTheKeysOfKafka010AreDeclared(): void
    {
        self::assertSame(17, ApiKeys::SASL_HANDSHAKE);
        self::assertSame(18, ApiKeys::API_VERSIONS);
        self::assertSame(19, ApiKeys::CREATE_TOPICS);
        self::assertSame(20, ApiKeys::DELETE_TOPICS);
    }

    /**
     * DeleteRecords (21) to AlterConfigs (33) arrived with Kafka 0.11
     */
    public function testTheKeysOfKafka011AreDeclared(): void
    {
        self::assertSame(21, ApiKeys::DELETE_RECORDS);
        self::assertSame(22, ApiKeys::INIT_PRODUCER_ID);
        self::assertSame(28, ApiKeys::TXN_OFFSET_COMMIT);
        self::assertSame(33, ApiKeys::ALTER_CONFIGS);
    }

    /**
     * AlterReplicaLogDirs (34), DescribeLogDirs (35), SaslAuthenticate (36) and CreatePartitions (37) arrived with
     * Kafka 1.0, the delegation token apis (38-41) and DeleteGroups (42) with Kafka 1.1
     */
    public function testTheKeysOfKafka1AreDeclared(): void
    {
        self::assertSame(34, ApiKeys::ALTER_REPLICA_LOG_DIRS);
        self::assertSame(35, ApiKeys::DESCRIBE_LOG_DIRS);
        self::assertSame(36, ApiKeys::SASL_AUTHENTICATE);
        self::assertSame(37, ApiKeys::CREATE_PARTITIONS);
        self::assertSame(38, ApiKeys::CREATE_DELEGATION_TOKEN);
        self::assertSame(41, ApiKeys::DESCRIBE_DELEGATION_TOKEN);
        self::assertSame(42, ApiKeys::DELETE_GROUPS);
    }

    /**
     * ElectPreferredLeaders (43) and IncrementalAlterConfigs (44) arrived with Kafka 2.2 / 2.3 and must not appear
     * on this branch
     */
    public function testNoApiKeyOfALaterKafkaIsDeclared(): void
    {
        $keys = new ReflectionClass(ApiKeys::class)->getConstants();

        self::assertSame(range(0, 42), array_values($keys));
        self::assertNotContains('ELECT_PREFERRED_LEADERS', array_keys($keys));
        self::assertNotContains('INCREMENTAL_ALTER_CONFIGS', array_keys($keys));
    }
}
