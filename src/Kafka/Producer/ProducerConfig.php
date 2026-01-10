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

use Protocol\Kafka\Common\ClientConfig as GeneralConfig;

/**
 * Producer config enumeration class
 */
final class ProducerConfig extends GeneralConfig
{
    public const string KEY_SERIALIZER            = 'key.serializer';
    public const string VALUE_SERIALIZER          = 'value.serializer';
    public const string ACKS                      = 'acks';
    public const string BUFFER_MEMORY             = 'buffer.memory';
    public const string COMPRESSION_TYPE          = 'compression.type';
    public const string RETRIES                   = 'retries';
    public const string BATCH_SIZE                = 'batch.size';
    public const string LINGER_MS                 = 'linger.ms';
    public const string MAX_REQUEST_SIZE          = 'max.request.size';
    public const string PARTITIONER_CLASS         = 'partitioner.class';
    public const RECEIVE_BUFFER_BYTES      = 'receive.buffer.bytes';
    public const string TIMEOUT_MS                = 'timeout.ms';
    public const METADATA_FETCH_TIMEOUT_MS = 'metadata.fetch.timeout.ms';
}
