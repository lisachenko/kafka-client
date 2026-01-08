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
 * @date   29.07.2016
 */

namespace Protocol\Kafka\Producer;

/**
 * Producer config enumeration class
 */
final class ProducerConfig
{
    public const BOOTSTRAP_SERVERS         = 'bootstrap.servers';
    public const string KEY_SERIALIZER            = 'key.serializer';
    public const string VALUE_SERIALIZER          = 'value.serializer';
    public const string ACKS                      = 'acks';
    public const string BUFFER_MEMORY             = 'buffer.memory';
    public const string COMPRESSION_TYPE          = 'compression.type';
    public const string RETRIES                   = 'retries';
    public const SSL_KEY_PASSWORD          = 'ssl.key.password';
    public const SSL_KEYSTORE_LOCATION     = 'ssl.keystore.location';
    public const SSL_KEYSTORE_PASSWORD     = 'ssl.keystore.password';
    public const string BATCH_SIZE                = 'batch.size';
    public const CLIENT_ID                 = 'client.id';
    public const CONNECTIONS_MAX_IDLE_MS   = 'connections.max.idle.ms';
    public const string LINGER_MS                 = 'linger.ms';
    public const string MAX_REQUEST_SIZE          = 'max.request.size';
    public const string PARTITIONER_CLASS         = 'partitioner.class';
    public const RECEIVE_BUFFER_BYTES      = 'receive.buffer.bytes';
    public const REQUEST_TIMEOUT_MS        = 'request.timeout.ms';
    public const SASL_MECHANISM            = 'sasl.mechanism';
    public const SECURITY_PROTOCOL         = 'security.protocol';
    public const SEND_BUFFER_BYTES         = 'send.buffer.bytes';
    public const string TIMEOUT_MS                = 'timeout.ms';
    public const METADATA_FETCH_TIMEOUT_MS = 'metadata.fetch.timeout.ms';
    public const METADATA_MAX_AGE_MS       = 'metadata.max.age.ms';
    public const RECONNECT_BACKOFF_MS      = 'reconnect.backoff.ms';
    public const RETRY_BACKOFF_MS          = 'retry.backoff.ms';
}
