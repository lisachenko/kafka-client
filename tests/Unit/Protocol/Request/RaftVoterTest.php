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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\AddRaftVoterRequestListener;
use Protocol\Kafka\Protocol\Request\AddRaftVoterRequest;
use Protocol\Kafka\Protocol\Request\AddRaftVoterRequestV0;
use Protocol\Kafka\Protocol\Request\AddRaftVoterResponse;
use Protocol\Kafka\Protocol\Request\AddRaftVoterResponseV0;
use Protocol\Kafka\Protocol\Request\RemoveRaftVoterRequest;
use Protocol\Kafka\Protocol\Request\RemoveRaftVoterResponse;

/**
 * Byte-exact tests for the two raft-voter apis of KIP-853, AddRaftVoter (key 80, v0 and v1) and RemoveRaftVoter
 * (key 81, v0)
 *
 * The frames are the ones the 4.3.1 node answered - a quorum at `kraft.version` 1 with the one voter
 * `ReplicaKey(id=1, directoryId=7QxpXGVBQTWaHmiG7IDrEQ)` - for the two requests that can never change it: an add
 * of the voter id 1, which is the 126, and a remove of the voter 4242, which is the 127.
 *
 * @see docs/protocol/4.3.md, sections "AddRaftVoter API (key 80, v0 and v1)" and "RemoveRaftVoter API (key 81, v0)"
 */
#[CoversClass(AddRaftVoterRequest::class)]
#[CoversClass(AddRaftVoterResponse::class)]
#[CoversClass(AddRaftVoterRequestV0::class)]
#[CoversClass(AddRaftVoterResponseV0::class)]
#[CoversClass(AddRaftVoterRequestListener::class)]
#[CoversClass(RemoveRaftVoterRequest::class)]
#[CoversClass(RemoveRaftVoterResponse::class)]
final class RaftVoterTest extends TestCase
{
    /**
     * The directory id of the one voter of the node, `7QxpXGVBQTWaHmiG7IDrEQ`
     */
    private const string VOTER_DIRECTORY_ID_HEX = 'ed0c695c654141359a1e6886ec80eb11';

    /**
     * An add of the voter 1 with its own key and an unreachable CONTROLLER endpoint.
     *
     *   ClusterId => 00 (null), TimeoutMs => 00007530 (30000), VoterId => 00000001
     *   VoterDirectoryId => ed0c695c654141359a1e6886ec80eb11
     *   Listeners => 02: Name 0b "CONTROLLER", Host 0a "127.0.0.1", Port 0001 (uint16), TAG_BUFFER 00
     *   TAG_BUFFER => 00
     */
    private const string ADD_REQUEST_HEX = '00000050'
        . '0050' . '0000' . '000010cd' . '0012' . '6b61666b612d636c69656e742d74312d3430' . '00'
        . '00'
        . '00007530'
        . '00000001'
        . self::VOTER_DIRECTORY_ID_HEX
        . '02' . '0b' . '434f4e54524f4c4c4552' . '0a' . '3132372e302e302e31' . '0001' . '00'
        . '00';

    public function testTheAddRequestIsTheVoterKeyAndItsEndpoints(): void
    {
        $request = new AddRaftVoterRequestV0(
            null,
            30000,
            1,
            (string) hex2bin(self::VOTER_DIRECTORY_ID_HEX),
            [new AddRaftVoterRequestListener('CONTROLLER', '127.0.0.1', 1)],
            'kafka-client-t1-40',
            4301
        );

        self::assertSame(ApiKeys::ADD_RAFT_VOTER, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertNull($request->getClusterId());
        self::assertSame(30000, $request->getTimeoutMs());
        self::assertSame(1, $request->getVoterId());
        self::assertSame('7QxpXGVBQTWaHmiG7IDrEQ', Uuid::toString($request->getVoterDirectoryId()));
        self::assertSame(['CONTROLLER'], array_keys($request->getListeners()));
        self::assertSame(self::ADD_REQUEST_HEX, bin2hex((string) $request));
    }

    /**
     * The port is the `uint16` of the protocol: a port above 32767 is not a negative number on the wire
     */
    public function testTheListenerPortIsAnUnsignedShort(): void
    {
        $request = new AddRaftVoterRequestV0(
            'c',
            1,
            7,
            Uuid::ZERO,
            [new AddRaftVoterRequestListener('CONTROLLER', 'h', 65535)]
        );

        self::assertStringEndsWith('0b434f4e54524f4c4c4552' . '0268' . 'ffff' . '00' . '00', bin2hex((string) $request));
    }

    /**
     * AddRaftVoter v1 (Kafka 4.2) is the frame of v0 plus the boolean `ack_when_committed` at the end of the body
     *
     * The two frames the 4.3.1 node answered with the 126, captured with the flag true and false (the node's voter
     * directory id `IpNOvhXLTxiMH2J_81GOUw`): one byte longer than v0, `01` or `00` in front of the tag buffer.
     */
    public function testTheVersionOneAppendsTheAcknowledgementModeToTheFrameOfVersionZero(): void
    {
        $directoryId = (string) hex2bin('22934ebe15cb4f188c1f627ff3518e53');
        $listeners   = [new AddRaftVoterRequestListener('CONTROLLER', '127.0.0.1', 1)];
        $body        = '00' . '00007530' . '00000001' . '22934ebe15cb4f188c1f627ff3518e53'
            . '02' . '0b' . '434f4e54524f4c4c4552' . '0a' . '3132372e302e302e31' . '0001' . '00';
        $header      = '0012' . '6b61666b612d636c69656e742d74312d3432' . '00';

        foreach ([[true, '01', 4201], [false, '00', 4202]] as [$ack, $flag, $correlationId]) {
            $request = new AddRaftVoterRequest(null, 30000, 1, $directoryId, $listeners, 'kafka-client-t1-42', $correlationId, $ack);

            self::assertSame(1, $request->getApiVersion());
            self::assertSame($ack, $request->isAckWhenCommitted());
            self::assertSame(
                '00000051' . '0050' . '0001' . sprintf('%08x', $correlationId) . $header . $body . $flag . '00',
                bin2hex((string) $request),
                'the frame the node answered, captured as addraftvoter.request.v1.*'
            );

            $v0 = new AddRaftVoterRequestV0(null, 30000, 1, $directoryId, $listeners, 'kafka-client-t1-42', $correlationId, $ack);
            self::assertSame(
                '00000050' . '0050' . '0000' . sprintf('%08x', $correlationId) . $header . $body . '00',
                bin2hex((string) $v0),
                'the flag of a version 0 request never reaches the wire'
            );
        }

        self::assertTrue(
            new AddRaftVoterRequest(null, 1, 7, Uuid::ZERO, [])->isAckWhenCommitted(),
            'the default of the field is true, the only behaviour of version 0'
        );
    }

    /**
     * The answer of v1 is the frame of v0: "Version 1 is the same as version 0"
     */
    public function testTheVersionOneAnswerIsTheFrameOfVersionZero(): void
    {
        $message = 'Aborted add voter operation for since API_VERSIONS returned an error BROKER_NOT_AVAILABLE';
        $hex     = '00000066' . '0000106b' . '00' . '00000000' . '0007' . '5a' . bin2hex($message) . '00';

        foreach ([AddRaftVoterResponse::class, AddRaftVoterResponseV0::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin($hex)));

            self::assertSame(KafkaException::REQUEST_TIMED_OUT, $response->errorCode);
            self::assertSame($message, $response->errorMessage);
            self::assertSame($hex, bin2hex((string) $response));
        }
        self::assertSame(1, AddRaftVoterResponse::VERSION);
        self::assertSame(0, AddRaftVoterResponseV0::VERSION);
    }

    /**
     * The 126 of the node for an add of the voter id it already has, with the voter set in the message
     */
    public function testTheAnswerIsTheCodeAndTheSentenceOfTheCheck(): void
    {
        $message = 'The voter id for ReplicaKey(id=1, directoryId=7QxpXGVBQTWaHmiG7IDrEQ) is already part of the set'
            . ' of voters [ReplicaKey(id=1, directoryId=7QxpXGVBQTWaHmiG7IDrEQ)].';
        $hex = '000000b0' . '000010cd' . '00' . '00000000' . '007e'
            . 'a301' . bin2hex($message)
            . '00';

        $response = AddRaftVoterResponseV0::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::DUPLICATE_VOTER, $response->errorCode);
        self::assertSame($message, $response->errorMessage, 'a compact string of 162 bytes: its length is two varint bytes');
        self::assertSame($hex, bin2hex((string) $response));
    }

    /**
     * RemoveRaftVoter is the voter key alone, and the 127 names the keys the voter set does hold
     */
    public function testTheRemoveRequestIsTheVoterKeyAlone(): void
    {
        $directoryId = (string) hex2bin('4456687c45a141717d2558c76c98d5b9');
        $request     = new RemoveRaftVoterRequest(null, 4242, $directoryId, 'kafka-client-t1-40', 4306);

        self::assertSame(ApiKeys::REMOVE_RAFT_VOTER, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(4242, $request->getVoterId());
        self::assertSame($directoryId, $request->getVoterDirectoryId());
        self::assertNull($request->getClusterId());
        self::assertSame(
            '00000033' . '0051' . '0000' . '000010d2' . '0012' . '6b61666b612d636c69656e742d74312d3430' . '00'
            . '00' . '00001092' . '4456687c45a141717d2558c76c98d5b9' . '00',
            bin2hex((string) $request)
        );

        $message = 'Cannot remove voter ReplicaKey(id=4242, directoryId=RFZofEWhQXF9JVjHbJjVuQ) from the set of'
            . ' voters [ReplicaKey(id=1, directoryId=7QxpXGVBQTWaHmiG7IDrEQ)]';
        $hex = '000000a7' . '000010d2' . '00' . '00000000' . '007f' . '9a01' . bin2hex($message) . '00';

        $response = RemoveRaftVoterResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::VOTER_NOT_FOUND, $response->errorCode);
        self::assertSame($message, $response->errorMessage);
        self::assertSame($hex, bin2hex((string) $response));
    }
}
