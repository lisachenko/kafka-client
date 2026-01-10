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

/**
 * General config, suitable for both producer and consumer
 */
class ClientConfig
{
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

    public const SSL_KEY_PASSWORD              = 'ssl.key.password';
    public const SSL_KEYSTORE_LOCATION         = 'ssl.keystore.location';
    public const SSL_KEYSTORE_PASSWORD         = 'ssl.keystore.password';
    public const CONNECTIONS_MAX_IDLE_MS       = 'connections.max.idle.ms';
    public const SASL_MECHANISM                = 'sasl.mechanism';
    public const SECURITY_PROTOCOL             = 'security.protocol';
    public const SEND_BUFFER_BYTES             = 'send.buffer.bytes';
    public const SSL_ENABLED_PROTOCOLS         = 'ssl.enabled.protocols';
    public const SSL_PROTOCOL                  = 'ssl.protocol';
    public const METADATA_MAX_AGE_MS           = 'metadata.max.age.ms';
    public const RECONNECT_BACKOFF_MS          = 'reconnect.backoff.ms';
    public const RETRY_BACKOFF_MS              = 'retry.backoff.ms';
}
