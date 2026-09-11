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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDir;

/**
 * DescribeLogDirs response object, version 1 (key 35)
 *
 * <pre>
 *   DescribeLogDirs Response (Version: 0 and 1) => throttle_time_ms [log_dirs]
 *     throttle_time_ms => INT32
 *     log_dirs         => error_code log_dir [topics]
 *       error_code => INT16
 *       log_dir    => STRING
 *       topics     => topic [partitions]
 *         topic      => STRING
 *         partitions => partition size offset_lag is_future
 *           partition  => INT32
 *           size       => INT64
 *           offset_lag => INT64
 *           is_future  => BOOLEAN
 * </pre>
 *
 * The api was born in Kafka 1.0, long after KIP-124 made `throttle_time_ms` the first field of every new answer, so
 * there is no version of it without one - and the answer has **no top-level error code** at all: every failure is a
 * property of a single directory, reported in the `error_code` of its entry.
 *
 * `log_dirs` holds one entry per directory of `log.dirs` of the answering broker, whether or not it holds any of
 * the requested replicas, and each entry only lists the replicas that really live in it. A partition that is being
 * moved by an AlterReplicaLogDirs request appears **twice**: as the current log of its source directory
 * (`is_future = false`) and as the future log of its destination (`is_future = true`).
 *
 * The one case in which the whole answer is empty is authorization: `KafkaApis.handleDescribeLogDirsRequest` @ 1.1.1
 * answers a client that may not `Describe` the cluster resource with an empty `log_dirs` array instead of an error
 * code, which the Java admin client translates back into 31 (ClusterAuthorizationFailed).
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `DESCRIBE_LOG_DIRS_RESPONSE_V1 =
 * DESCRIBE_LOG_DIRS_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see DescribeLogDirsResponseV0} is the same frame with the version field of Kafka 1.0.
 *
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0 to v2)"
 */
class DescribeLogDirsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * The version 2 of Kafka 2.6 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Every log directory of the answering broker, indexed by its absolute path
     *
     * @var array<string, DescribeLogDirsResponseLogDir>
     */
    public array $logDirs = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'logDirs'        => ['logDir' => DescribeLogDirsResponseLogDir::class],
        ];
    }
}
