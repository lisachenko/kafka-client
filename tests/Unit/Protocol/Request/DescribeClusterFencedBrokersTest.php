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
use Protocol\Kafka\Admin\ClusterDescription;
use Protocol\Kafka\Admin\EndpointType;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeClusterBroker;
use Protocol\Kafka\Protocol\Data\DescribeClusterBrokerV1;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequest;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponse;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponseV1;

/**
 * Byte-exact tests for the version 2 of DescribeCluster (key 60, Kafka 4.0, KIP-1073): the fenced brokers.
 *
 * The request appends `include_fenced_brokers` behind the `endpoint_type` of the version 1, and every broker of the
 * answer appends `is_fenced` behind its rack. The frames are the ones the 4.3.1 node answered on its PLAINTEXT
 * listener; a one-node cluster has no fenced broker to show, so the flag of its one broker is false whether the
 * request asked for the fenced ones or not.
 *
 * @see docs/protocol/4.3.md, section "The fenced brokers of KIP-1073 (v2)"
 */
#[CoversClass(DescribeClusterRequest::class)]
#[CoversClass(DescribeClusterRequestV1::class)]
#[CoversClass(DescribeClusterResponse::class)]
#[CoversClass(DescribeClusterResponseV1::class)]
#[CoversClass(DescribeClusterBroker::class)]
#[CoversClass(DescribeClusterBrokerV1::class)]
#[CoversClass(ClusterDescription::class)]
final class DescribeClusterFencedBrokersTest extends TestCase
{
    /**
     * The version 2 answer of the node to `include_fenced_brokers = true`: the one broker, not fenced.
     *
     *   ThrottleTimeMs => 00000000, ErrorCode => 0000, ErrorMessage => 00 (null), EndpointType => 01
     *   ClusterId => 17 "fW0-nn68RxyTd25Kp36OHg", ControllerId => 00000001
     *   Brokers => 02: BrokerId 00000001, Host 0a "127.0.0.1", Port 00002384, Rack 00, IsFenced 00, TAG_BUFFER 00
     *   ClusterAuthorizedOperations => 80000000, TAG_BUFFER => 00
     */
    private const string RESPONSE_V2_HEX = '00000043'
        . '0000106a'
        . '00'
        . '00000000'
        . '0000'
        . '00'
        . '01'
        . '17' . '6657302d6e6e3638527879546432354b7033364f4867'
        . '00000001'
        . '02'
        . '00000001' . '0a' . '3132372e302e302e31' . '00002384' . '00' . '00' . '00'
        . '80000000'
        . '00';

    /**
     * The flag is the one byte the version 2 appended to the request
     */
    public function testTheRequestAppendsTheFencedBrokersFlag(): void
    {
        $request = new DescribeClusterRequest(false, 'kafka-client-t1-40', 4202, EndpointType::Broker, true);

        self::assertSame(ApiKeys::DESCRIBE_CLUSTER, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion());
        self::assertTrue($request->includesFencedBrokers());
        self::assertSame(
            '00000021003c00020000106a00126b61666b612d636c69656e742d74312d34300000010100',
            bin2hex((string) $request),
            'the acl flag 00, the endpoint type 01, the fenced flag 01 and the tag buffer of the body'
        );

        $version1 = new DescribeClusterRequestV1(false, 'kafka-client-t1-40', 4202, EndpointType::Broker, true);

        self::assertSame(
            '00000020003c00010000106a00126b61666b612d636c69656e742d74312d343000000100',
            bin2hex((string) $version1),
            'a version 1 frame has no field for the flag, and the caller\'s wish never reaches the wire'
        );
        self::assertFalse(new DescribeClusterRequest()->includesFencedBrokers(), 'the default leaves them out');
    }

    /**
     * Every broker of the version 2 answer carries `is_fenced`
     */
    public function testEveryBrokerOfTheAnswerCarriesItsFencedFlag(): void
    {
        $response = DescribeClusterResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertNull($response->errorMessage);
        self::assertSame('fW0-nn68RxyTd25Kp36OHg', $response->clusterId);
        self::assertInstanceOf(DescribeClusterBroker::class, $response->brokers[1]);
        self::assertNotInstanceOf(DescribeClusterBrokerV1::class, $response->brokers[1]);
        self::assertFalse($response->brokers[1]->isFenced);
        self::assertSame(self::RESPONSE_V2_HEX, bin2hex((string) $response));
    }

    /**
     * The brokers of the versions 0 and 1 have no flag, and the two frames are one byte per broker apart
     */
    public function testTheBrokersBelowVersionTwoHaveNoFlag(): void
    {
        self::assertArrayHasKey('isFenced', DescribeClusterBroker::getScheme());
        self::assertArrayNotHasKey('isFenced', DescribeClusterBrokerV1::getScheme());
        self::assertSame(
            ['brokerId' => DescribeClusterBrokerV1::class],
            DescribeClusterResponseV1::getScheme()['brokers']
        );
        self::assertSame(
            ['brokerId' => DescribeClusterBrokerV1::class],
            DescribeClusterResponseV0::getScheme()['brokers']
        );

        // The version 1 answer of the same node to the same question, captured next to the version 2 one
        $version1 = '00000042' . '0000106e' . '00' . '00000000' . '0000' . '00' . '01'
            . '17' . '6657302d6e6e3638527879546432354b7033364f4867'
            . '00000001'
            . '02' . '00000001' . '0a' . '3132372e302e302e31' . '00002384' . '00' . '00'
            . '80000000'
            . '00';
        $response = DescribeClusterResponseV1::unpack(new StringStream((string) hex2bin($version1)));

        self::assertInstanceOf(DescribeClusterBrokerV1::class, $response->brokers[1]);
        self::assertFalse($response->brokers[1]->isFenced, 'the default of a broker nobody fenced');
        self::assertSame($version1, bin2hex((string) $response));
    }

    /**
     * The description lists the fenced ids next to the nodes, because a node of a Metadata answer has no flag
     */
    public function testTheDescriptionTellsTheFencedBrokersApart(): void
    {
        $description = new ClusterDescription('c', 1, [], ClusterDescription::OPERATIONS_NOT_REQUESTED, EndpointType::Broker, [2]);

        self::assertTrue($description->isFenced(2));
        self::assertFalse($description->isFenced(1));
        self::assertSame([], new ClusterDescription('c', 1, [])->fencedNodeIds);
    }
}
