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

namespace Protocol\Kafka\Admin;

/**
 * Which half of a KRaft cluster a {@see AdminClient::describeCluster()} asks about (Kafka 3.7, KIP-919)
 *
 * `org.apache.kafka.clients.admin.EndpointType` of the Java client, an enum of the same three values. A cluster
 * that runs without ZooKeeper has **two** sets of nodes - the brokers that serve clients and the controllers that
 * hold the metadata log - and until Kafka 3.7 the protocol had no way of naming the second set: DescribeCluster
 * v0 always described the brokers, and the controller listeners were something a client had to be configured
 * with. KIP-919 gave the request an `endpoint_type` byte and the answer one of its own, so that the same api
 * describes either set.
 *
 * The value {@see self::Unknown} is the zero of the enum, which `EndpointType.fromId` @ 3.9.2 answers for every
 * byte that is neither 1 nor 2 and which no client sends; a server that is asked for it answers **115**
 * (`UnsupportedEndpointType`).
 *
 * @see docs/protocol/3.9.md, section "The endpoint type of KIP-919 (v1)"
 */
enum EndpointType: int
{
    /**
     * Not one of the two sets of nodes, the value `EndpointType.fromId` answers for any other byte
     */
    case Unknown = 0;

    /**
     * The brokers of the cluster, which is what every version 0 request describes
     */
    case Broker = 1;

    /**
     * The controllers of the cluster, i.e. the voters of its metadata quorum (KIP-919)
     */
    case Controller = 2;

    /**
     * Returns the endpoint type of a byte of the wire, {@see self::Unknown} for one the api does not define
     *
     * `EndpointType.fromId` @ 3.9.2, which maps everything but 1 and 2 to `UNKNOWN`.
     */
    public static function fromId(int $id): self
    {
        return self::tryFrom($id) ?? self::Unknown;
    }
}
