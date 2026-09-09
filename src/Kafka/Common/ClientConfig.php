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
 * @date   01.08.2016
 */

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Common\Security\SslProtocol;

/**
 * General config, suitable for both producer and consumer
 *
 * Kafka 0.9.0.0 is the release that gave a broker more than one listener: `security.protocol` selects the transport
 * this client connects with, the `ssl.*` options configure the TLS handshake it performs before the first request,
 * and the `sasl.*` options configure the authentication exchange that Kafka 0.10.0 made part of the protocol
 * (`SaslHandshake`, api key 17).
 *
 * @see docs/protocol/0.11.0.md, section "Transport security (SSL)"
 */
class ClientConfig
{
    protected static $generalConfiguration = [
        ClientConfig::BOOTSTRAP_SERVERS            => [],
        ClientConfig::CLIENT_ID                    => 'PHP/Kafka',
        ClientConfig::STREAM_PERSISTENT_CONNECTION => false,
        ClientConfig::STREAM_ASYNC_CONNECT         => false,
        ClientConfig::METADATA_MAX_AGE_MS          => 300000,
        ClientConfig::RECEIVE_BUFFER_BYTES         => 32768,
        ClientConfig::SEND_BUFFER_BYTES            => 131072,

        ClientConfig::SECURITY_PROTOCOL            => SecurityProtocol::PLAINTEXT,
        ClientConfig::SSL_PROTOCOL                 => SslProtocol::TLS,
        ClientConfig::SSL_ENABLED_PROTOCOLS        => null,
        ClientConfig::SSL_CA_CERT_LOCATION         => null,
        ClientConfig::SSL_CLIENT_CERT_LOCATION     => null,
        ClientConfig::SSL_KEY_LOCATION             => null,
        ClientConfig::SSL_KEY_PASSWORD             => null,

        ClientConfig::SASL_MECHANISM               => SaslMechanism::PLAIN,
        ClientConfig::SASL_USERNAME                => null,
        ClientConfig::SASL_PASSWORD                => null,

        ClientConfig::CONNECTIONS_MAX_IDLE_MS   => 540000,
        ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
        ClientConfig::METADATA_FETCH_TIMEOUT_MS => 60000,
        ClientConfig::RECONNECT_BACKOFF_MS      => 50,
        ClientConfig::RETRY_BACKOFF_MS          => 100,
        ClientConfig::RETRIES                   => 2,
        ClientConfig::OFFSETS_STORAGE           => 'kafka',
    ];

    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     */
    public const BOOTSTRAP_SERVERS = 'bootstrap.servers';

    /**
     * An id string to pass to the server when making requests.
     *
     * The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical
     * application name to be included in server-side request logging.
     */
    public const CLIENT_ID = 'client.id';

    /**
     * The configuration controls the maximum amount of time the client will wait for the response of a request.
     *
     * If the response is not received before the timeout elapses the client will resend the request if necessary or
     * fail the request if retries are exhausted.
     */
    public const REQUEST_TIMEOUT_MS = 'request.timeout.ms';

    /**
     * Where the consumer offsets are stored: in Kafka itself or in ZooKeeper.
     *
     * Kafka 0.8.2 introduced Kafka-based offset storage (OffsetCommit/OffsetFetch v1) while keeping the ZooKeeper
     * storage of 0.8.1 available through v0 of the same requests. This option selects the storage that the client
     * uses; only the `kafka` value is meaningful for a broker-only client.
     */
    public const OFFSETS_STORAGE = 'offsets.storage';

    /**
     * Offsets are committed to and fetched from the offset coordinator (OffsetCommit/OffsetFetch v1)
     */
    public const OFFSETS_STORAGE_KAFKA = 'kafka';

    /**
     * Offsets are committed to and fetched from ZooKeeper (OffsetCommit/OffsetFetch v0)
     */
    public const OFFSETS_STORAGE_ZOOKEEPER = 'zookeeper';

    /**
     * Should client use persistent connection to the cluster or not
     *
     * (PHP Only option)
     */
    public const STREAM_PERSISTENT_CONNECTION = 'stream.persistent.connection';

    /**
     * Should client use asynchronous connection to the broker
     *
     * (PHP Only option)
     */
    public const STREAM_ASYNC_CONNECT = 'stream.async.connect';

    /**
     * File name that stores the metadata, this file will be effectively cached by the Opcode cache in production
     *
     * (PHP Only option)
     */
    public const METADATA_CACHE_FILE = 'metadata.cache.file';

    /**
     * The first time data is sent to the broker we must fetch metadata about that topic to know which servers host the
     * topic's partitions. This fetch to succeed before throwing an exception back to the client.
     */
    public const METADATA_FETCH_TIMEOUT_MS = 'metadata.fetch.timeout.ms';

    /**
     * The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any
     * partition leadership changes to proactively discover any new brokers or partitions.
     *
     * This drives both the in-memory metadata of {@see Cluster} and the lifetime of the `metadata.cache.file`, if
     * one is configured: a cache entry that is older than this is ignored and the metadata is fetched again.
     */
    public const METADATA_MAX_AGE_MS = 'metadata.max.age.ms';

    /**
     * The size of the TCP send buffer (SO_SNDBUF) to use when sending data.
     */
    public const SEND_BUFFER_BYTES = 'send.buffer.bytes';

    /**
     * The size of the TCP receive buffer (SO_RCVBUF) to use when reading data.
     */
    public const RECEIVE_BUFFER_BYTES = 'receive.buffer.bytes';

    /**
     * Close idle connections after the number of milliseconds specified by this config.
     *
     * A Kafka broker drops a connection that has been idle for `connections.max.idle.ms` on its side as well, so a
     * cached connection that is older than this is closed before it is handed out again, rather than failing in the
     * middle of the next request.
     *
     * @see \Protocol\Kafka\Network\ConnectionFactory
     */
    public const CONNECTIONS_MAX_IDLE_MS = 'connections.max.idle.ms';

    /**
     * The amount of time to wait before attempting to reconnect to a given host.
     *
     * This avoids repeatedly connecting to a host in a tight loop.
     */
    public const RECONNECT_BACKOFF_MS = 'reconnect.backoff.ms';

    /**
     * The amount of time to wait before attempting to retry a failed request to a given topic partition.
     *
     * This avoids repeatedly sending requests in a tight loop under some failure scenarios.
     */
    public const RETRY_BACKOFF_MS = 'retry.backoff.ms';

    /**
     * How often a request that failed with a retriable error is sent again before it is reported to the caller.
     *
     * The client refreshes the cluster metadata before each of those attempts, because a retriable error - 3
     * UnknownTopicOrPartition, 5 LeaderNotAvailable, 6 NotLeaderForPartition or a dropped connection - almost always
     * means that the cached leader of a partition is not the current one any more.
     *
     * @see \Protocol\Kafka\Network\RetryPolicy
     */
    public const RETRIES = 'retries';

    /**
     * Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
     *
     * A broker of Kafka 0.10.2.2 binds one listener per protocol and answers the identical request set on each of
     * them, so this option only selects the transport - never the wire format. The two SASL protocols additionally
     * authenticate the connection with `sasl.mechanism`, `sasl.username` and `sasl.password` before the first
     * ordinary request is sent.
     *
     * @see \Protocol\Kafka\Common\Security\SecurityProtocol
     */
    public const SECURITY_PROTOCOL = 'security.protocol';

    /**
     * The SSL protocol used to generate the SSLContext. Default setting is TLS, which is fine for most cases.
     * Allowed values are TLS, TLSv1_1 and TLSv1_2, SSL, SSLv2 and SSLv3, but the usage of the last three is
     * discouraged due to known security vulnerabilities.
     *
     * @see \Protocol\Kafka\Common\Security\SslProtocol
     */
    public const SSL_PROTOCOL = 'ssl.protocol';

    /**
     * The list of protocols enabled for SSL connections, as a list of {@see SslProtocol} values.
     *
     * When it is set, the handshake offers exactly those versions and `ssl.protocol` is ignored; when it is empty or
     * missing, `ssl.protocol` alone decides. The Java client of 0.10.2.2 defaults to `TLSv1.2,TLSv1.1,TLSv1`, which
     * is what the `TLS` default of `ssl.protocol` negotiates here.
     */
    public const SSL_ENABLED_PROTOCOLS = 'ssl.enabled.protocols';

    /**
     * The location of the PEM file with the certificates the broker certificate is verified against.
     *
     * This is the counterpart of the `ssl.truststore.location` of the Java client, which stores the same
     * certificates in a JKS keystore. Without it the certificate stores of the system are used.
     *
     * (PHP Only option)
     */
    public const SSL_CA_CERT_LOCATION = 'ssl.ca.cert.location';

    /**
     * Path to local certificate file on filesystem. It must be a PEM encoded file which contains your
     * certificate and private key. It can optionally contain the certificate chain of issuers.
     * The private key also may be contained in a separate file specified by SSL_KEY_LOCATION.
     *
     * Only needed for a broker configured with `ssl.client.auth=required` (two-way authentication).
     *
     * (PHP Only option)
     */
    public const SSL_CLIENT_CERT_LOCATION = 'ssl.client.cert.location';

    /**
     * The location of the private key file. This is optional for client and can be used for two-way
     * authentication for client.
     */
    public const SSL_KEY_LOCATION = 'ssl.key.location';

    /**
     * The password of the private key. This is optional for client.
     */
    public const SSL_KEY_PASSWORD = 'ssl.key.password';

    /**
     * SASL mechanism used for client connections, the `Mechanism` of the SaslHandshake request.
     *
     * The default is `PLAIN`, which is also the only mechanism this client implements; a Kafka 0.10.2.2 broker can
     * enable GSSAPI, PLAIN and the two SCRAM mechanisms, and answers a mechanism it has not enabled with the error
     * code 33 (UnsupportedSaslMechanism) plus the list of the ones it has.
     *
     * Only meaningful together with `security.protocol = SASL_PLAINTEXT` or `SASL_SSL`.
     *
     * @see \Protocol\Kafka\Common\Security\SaslMechanism
     */
    public const SASL_MECHANISM = 'sasl.mechanism';

    /**
     * User name of the SASL/PLAIN credentials, the `authcid` of the token (`user_<name>` in the JAAS file of a broker).
     *
     * The Java client carries the credentials in a JAAS configuration - a `sasl.jaas.config` entry or the
     * `java.security.auth.login.config` system property naming a `KafkaClient` login module. That file format is
     * not reproduced here: PHP has no JAAS, and a login module is a Java class. The two options below are the whole
     * equivalent of the `username`/`password` of `PlainLoginModule`.
     *
     * (PHP Only option)
     */
    public const SASL_USERNAME = 'sasl.username';

    /**
     * Password of the SASL/PLAIN credentials, sent in clear text inside the token.
     *
     * With `SASL_PLAINTEXT` it travels over an unencrypted connection - use it on a trusted network only, and
     * prefer `SASL_SSL`, which performs the very same exchange inside the TLS channel.
     *
     * (PHP Only option)
     */
    public const SASL_PASSWORD = 'sasl.password';

    /**
     * Returns default configuration
     */
    public static function getDefaultConfiguration(): array
    {
        return self::$generalConfiguration;
    }
}
