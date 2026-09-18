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

namespace Protocol\Kafka\Tests\Fixture;

use RuntimeException;

/**
 * Creates and drives a group of the **KIP-848 consumer protocol** with hand-built ConsumerGroupHeartbeat frames.
 *
 * The api (key 68, Kafka 3.5) is the last wave of this line and has no classes yet, but two waves need a group of
 * the new protocol before it arrives: the **113** `StaleMemberEpoch` of an OffsetCommit v9 (Kafka 3.6) and the
 * **113** and **25** of an OffsetFetch v9 (Kafka 3.7) exist for the members of such a group alone, and no classic
 * group produces either of them. The frame is therefore packed here, on top of {@see RawApiProbe}, and this class
 * is the one place of the suite that knows its layout.
 *
 * The api is **flexible from its version 0 on**, so every string and array of it is compact and every structure
 * ends in a tag buffer. What the coordinator insists on, measured on the 3.9.2 node:
 *
 * * a heartbeat that (re-)joins carries the epoch **0**, a subscription (`subscribed_topic_names`), a rebalance
 *   timeout and an **empty** `topic_partitions` array - the `null` the field defaults to is refused **42**
 *   `InvalidRequest` with "TopicPartitions must be empty when (re-)joining."
 *   (`GroupMetadataManager.throwIfConsumerGroupHeartbeatRequestIsInvalid` @ 3.9.2);
 * * a member that names no member id is given one by the coordinator, a member that names a uuid of its own keeps
 *   it, and the epoch the join is answered with is the group epoch, not the 0 it sent;
 * * a member leaves with the epoch **-1** ({@see self::LEAVE_MEMBER_EPOCH}) and a `null` `topic_partitions`, and a
 *   group that still holds a member cannot be deleted - every test that creates one has to leave it again.
 *
 * @see \Protocol\Kafka\Tests\Integration\MemberEpochCommitApiTest
 * @see \Protocol\Kafka\Tests\Integration\MemberEpochFetchApiTest
 * @see docs/protocol/3.9.md, section "The member epoch of KIP-848 (v9)"
 * @see docs/protocol/3.9.md, section "The member id and epoch of KIP-848 (v9)"
 */
final class ConsumerGroupHeartbeatProbe
{
    /**
     * Api key of ConsumerGroupHeartbeat (KIP-848), which this line implements in its last wave
     */
    public const int API_KEY = 68;

    /**
     * Member epoch of a heartbeat that joins a group
     */
    public const int JOIN_MEMBER_EPOCH = 0;

    /**
     * Member epoch of a heartbeat that leaves one
     */
    public const int LEAVE_MEMBER_EPOCH = -1;

    /**
     * @param string $bootstrapServer `host:port` of the node, without a scheme
     * @param float  $timeout         Seconds to wait for an answer
     */
    public function __construct(
        private readonly string $bootstrapServer,
        private readonly float $timeout = 10.0
    ) {}

    /**
     * Returns the member id a KIP-848 consumer generates for itself, the uuid the Java client sends
     */
    public static function newMemberId(): string
    {
        return sprintf(
            '%04x%04x-%04x-4%03x-%04x-%04x%04x%04x',
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0x0FFF),
            random_int(0, 0x3FFF) | 0x8000,
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF)
        );
    }

    /**
     * Creates the group - or joins the existing one - and returns the member epoch the coordinator answered with
     *
     * @param list<string> $topics Subscription of the member, which a join has to carry
     *
     * @throws RuntimeException If the node refused the heartbeat
     */
    public function join(
        string $groupId,
        string $memberId,
        array $topics,
        int $rebalanceTimeoutMs,
        int $correlationId
    ): int {
        $answer = $this->send(
            $groupId,
            $memberId,
            self::JOIN_MEMBER_EPOCH,
            $rebalanceTimeoutMs,
            $topics,
            $correlationId
        );

        if ($answer['errorCode'] !== 0) {
            throw new RuntimeException(
                "The node refused the heartbeat that creates the group '{$groupId}': "
                . $answer['errorCode'] . ' ' . var_export($answer['errorMessage'], true)
            );
        }
        if ($answer['memberEpoch'] <= 0) {
            throw new RuntimeException("The member joined '{$groupId}' with the epoch {$answer['memberEpoch']}");
        }

        return $answer['memberEpoch'];
    }

    /**
     * Takes the member out of its group again, so that the group can be deleted
     *
     * @return array{errorCode: int, errorMessage: string|null, memberId: string|null, memberEpoch: int}
     */
    public function leave(string $groupId, string $memberId, int $correlationId): array
    {
        return $this->send($groupId, $memberId, self::LEAVE_MEMBER_EPOCH, -1, null, $correlationId);
    }

    /**
     * Sends one hand-built ConsumerGroupHeartbeat v0 frame and decodes the fields the suites need
     *
     * @param list<string>|null $topics Subscription of the member; a join has to carry one, a leave sends null
     *
     * @return array{errorCode: int, errorMessage: string|null, memberId: string|null, memberEpoch: int}
     *
     * @throws RuntimeException If the node did not answer the frame at all
     */
    public function send(
        string $groupId,
        string $memberId,
        int $memberEpoch,
        int $rebalanceTimeoutMs,
        ?array $topics,
        int $correlationId
    ): array {
        $body = RawApiProbe::compactString($groupId)
            . RawApiProbe::compactString($memberId)
            . RawApiProbe::int32($memberEpoch)
            . RawApiProbe::compactString(null)           // instance id
            . RawApiProbe::compactString(null)           // rack id
            . RawApiProbe::int32($rebalanceTimeoutMs);
        if ($topics === null) {
            $body .= RawApiProbe::compactArray(null);
        } else {
            $body .= RawApiProbe::compactArray(count($topics));
            foreach ($topics as $name) {
                $body .= RawApiProbe::compactString($name);
            }
        }
        $body .= RawApiProbe::compactString(null)        // server assignor
            . RawApiProbe::compactArray($memberEpoch === self::JOIN_MEMBER_EPOCH ? 0 : null)
            . RawApiProbe::tagBuffer();

        $probe  = new RawApiProbe($this->bootstrapServer);
        $answer = $probe->send(
            self::API_KEY,
            0,
            $body,
            $correlationId,
            RawApiProbe::HEADER_V2,
            $this->timeout
        );
        $probe->close();

        if ($answer['status'] !== RawApiProbe::ANSWERED) {
            throw new RuntimeException("The node did not answer the heartbeat: {$answer['status']}");
        }
        if ($answer['correlationId'] !== $correlationId) {
            throw new RuntimeException('The node answered another correlation id than the heartbeat carried');
        }

        return self::decode($answer['body']);
    }

    /**
     * Reads the leading fields of a ConsumerGroupHeartbeat answer, the tag buffer of the response header included
     *
     * @return array{errorCode: int, errorMessage: string|null, memberId: string|null, memberEpoch: int}
     */
    private static function decode(string $body): array
    {
        $offset = 0;
        $varint = static function () use ($body, &$offset): int {
            $value = 0;
            $shift = 0;
            while (true) {
                $byte = ord($body[$offset++]);
                $value |= ($byte & 0x7F) << $shift;
                if (($byte & 0x80) === 0) {
                    return $value;
                }
                $shift += 7;
            }
        };
        $string = static function () use ($body, &$offset, $varint): ?string {
            $length = $varint();
            if ($length === 0) {
                return null;
            }
            $value = substr($body, $offset, $length - 1);
            $offset += $length - 1;

            return $value;
        };

        $varint();                                                  // tag buffer of the response header v1
        $offset += 4;                                               // throttle time
        $errorCode = (int) unpack('n', substr($body, $offset, 2))[1];
        $offset += 2;
        $errorMessage = $string();
        $memberId     = $string();
        $memberEpoch  = (int) unpack('N', substr($body, $offset, 4))[1];

        return [
            'errorCode'    => $errorCode > 0x7FFF ? $errorCode - 0x10000 : $errorCode,
            'errorMessage' => $errorMessage,
            'memberId'     => $memberId,
            'memberEpoch'  => $memberEpoch > 0x7FFFFFFF ? $memberEpoch - 0x100000000 : $memberEpoch,
        ];
    }
}
