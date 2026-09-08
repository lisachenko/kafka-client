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

namespace Protocol\Kafka\Tests\Unit\Consumer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Consumer\Subscription;

/**
 * Byte-exact tests for the member metadata of the `consumer` group protocol.
 *
 * The reference bytes of {@see testPacksTheMetadataThatTheJavaClientSends()} are the ones a Kafka 0.9.0.1 broker
 * relayed for a `kafka-console-consumer.sh --new-consumer` member, read back with a raw DescribeGroups v0 frame.
 *
 * @see docs/protocol/0.9.0.md, section "Consumer group protocol (protocol_type = consumer)"
 */
#[CoversClass(Subscription::class)]
final class SubscriptionTest extends TestCase
{
    /**
     * Topic of the group whose member metadata was captured from the broker
     */
    private const string CAPTURED_TOPIC = 't6-consumer-protocol';

    /**
     * `member_metadata` of the Java 0.9.0.1 consumer that subscribed to that topic
     */
    private const string CAPTURED_METADATA = '0000'                                      // Version = 0
        . '00000001'                                                                     // one topic
        . '0014' . '74362d636f6e73756d65722d70726f746f636f6c'                            // "t6-consumer-protocol"
        . '00000000';                                                                    // UserData = empty bytes

    public function testPacksTheMetadataThatTheJavaClientSends(): void
    {
        $subscription = new Subscription([self::CAPTURED_TOPIC]);

        self::assertSame(self::CAPTURED_METADATA, bin2hex($subscription->pack()));
    }

    public function testUnpacksTheMetadataOfTheJavaClient(): void
    {
        $subscription = Subscription::unpack((string) hex2bin(self::CAPTURED_METADATA));

        self::assertSame(Subscription::VERSION, $subscription->version);
        self::assertSame([self::CAPTURED_TOPIC], $subscription->topics);
        self::assertSame('', $subscription->userData);
    }

    public function testKeepsTheOrderOfSeveralTopics(): void
    {
        $subscription = new Subscription(['foo', 'bar']);

        self::assertSame(
            '0000'                    // Version = 0
            . '00000002'              // two topics
            . '0003' . '666f6f'       // "foo"
            . '0003' . '626172'       // "bar"
            . '00000000',             // UserData = empty bytes
            bin2hex($subscription->pack())
        );
        self::assertSame(['foo', 'bar'], Subscription::unpack($subscription->pack())->topics);
    }

    public function testSubscribesToNoTopicAtAll(): void
    {
        $subscription = new Subscription([]);

        self::assertSame('0000' . '00000000' . '00000000', bin2hex($subscription->pack()));
        self::assertSame([], Subscription::unpack($subscription->pack())->topics);
    }

    public function testCarriesTheUserDataOfACustomAssignor(): void
    {
        $subscription = new Subscription(['foo'], userData: 'rack-1');

        self::assertSame(
            '0000' . '00000001' . '0003' . '666f6f' . '00000006' . '7261636b2d31',
            bin2hex($subscription->pack())
        );
        self::assertSame('rack-1', Subscription::unpack($subscription->pack())->userData);
    }

    public function testWritesAndReadsTheNullUserDataOfTheSpecification(): void
    {
        $subscription = new Subscription(['foo'], userData: null);

        // The `bytes` primitive is nullable and -1 is its null value; the Java client of 0.9 defaults the field to
        // an empty byte array instead, but accepts the null one
        self::assertSame('0000' . '00000001' . '0003' . '666f6f' . 'ffffffff', bin2hex($subscription->pack()));
        self::assertNull(Subscription::unpack($subscription->pack())->userData);
    }

    public function testKeepsAHigherVersionOfTheStructure(): void
    {
        // The Java client parses a newer version with the layout of version 0, so a version it does not know has to
        // survive the round trip instead of being normalized away
        $subscription = new Subscription(['foo'], version: 100);
        $restored     = Subscription::unpack($subscription->pack());

        self::assertSame(100, $restored->version);
        self::assertSame(['foo'], $restored->topics);
    }
}
