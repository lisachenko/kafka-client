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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDir;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDirV3;

/**
 * DescribeLogDirs response object, version 4 (key 35)
 *
 * <pre>
 *   DescribeLogDirs Response (Version: 4) => throttle_time_ms error_code [log_dirs]
 *     throttle_time_ms => INT32
 *     error_code       => INT16                      -- since version 3
 *     log_dirs         => error_code log_dir [topics] total_bytes usable_bytes
 *       error_code => INT16
 *       log_dir    => STRING
 *       topics     => topic [partitions]
 *         topic      => STRING
 *         partitions => partition size offset_lag is_future
 *           partition  => INT32
 *           size       => INT64
 *           offset_lag => INT64
 *           is_future  => BOOLEAN
 *       total_bytes  => INT64                        -- since version 4
 *       usable_bytes => INT64                        -- since version 4
 * </pre>
 *
 * The api was born in Kafka 1.0, long after KIP-124 made `throttle_time_ms` the first field of every new answer, so
 * there is no version of it without one - and the answer of the versions 0 to 2 has **no top-level error code** at
 * all: every failure is a property of a single directory, reported in the `error_code` of its entry.
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
 * **Kafka 3.2 added the version 3 and with it the top-level `error_code`** ("Version 3 adds the top-level ErrorCode
 * field" of `DescribeLogDirsResponse.json` @ 3.2.3), an `int16` between `throttle_time_ms` and `log_dirs` that
 * carries exactly that refusal: `KafkaApis.handleDescribeLogDirsRequest` @ 3.9.2 answers a principal that may not
 * `Describe` the cluster resource with 31 and an empty `log_dirs` array, where the same refusal below version 3 is
 * the empty array alone. Every other failure stays a property of one directory.
 * {@see \Protocol\Kafka\Admin\AdminClient::describeLogDirs()} raises {@see self::$errorCode} as the exception of
 * the whole request. {@see DescribeLogDirsResponseV2} decodes the answer of the versions 2 and below, which has no
 * such field.
 *
 * **Kafka 3.3 added the version 4 and with it the two sizes of KIP-827** ("Version 4 adds the TotalBytes and
 * UsableBytes fields" of `DescribeLogDirsResponse.json` @ 3.3.2): two `int64` at the **end of every directory
 * entry**, behind its topics, that describe the volume the directory sits on rather than the directory itself -
 * `LogManager.describeLogDirs` @ 3.9.2 fills them with `File.getTotalSpace` and `File.getUsableSpace`. Two
 * directories of the same filesystem therefore answer the same two numbers, and a directory the broker could not
 * measure keeps the default {@see \Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDir::UNKNOWN_BYTES}.
 * The request is unchanged, so the version alone asks for them; {@see DescribeLogDirsResponseV3} decodes the
 * answer of a broker below Kafka 3.3, whose directory entries end with their topics.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `DESCRIBE_LOG_DIRS_RESPONSE_V1 =
 * DESCRIBE_LOG_DIRS_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see DescribeLogDirsResponseV0} is the same frame with the version field of Kafka 1.0.
 *
 * @see docs/protocol/4.3.md, section "DescribeLogDirs API (key 35, v0 to v4)"
 */
class DescribeLogDirsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;

    /**
     * The version 2 of Kafka 2.6 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * The version 3 of Kafka 3.2 is the first one whose answer carries a top-level error code
     */
    public const int TOP_LEVEL_ERROR_VERSION = 3;

    /**
     * The version 4 of Kafka 3.3 is the first one whose directories carry the two sizes of their volume (KIP-827)
     */
    public const int VOLUME_SIZE_VERSION = 4;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the whole request, 31 (ClusterAuthorizationFailed) for a principal that may not describe the cluster
     *
     * The field exists from the version 3 of Kafka 3.2 on and stays 0 for every answer below it.
     */
    public int $errorCode = KafkaException::NO_ERROR;

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
        $body   = ['throttleTimeMs' => BinarySchema::TYPE_INT32];

        if (static::VERSION >= self::TOP_LEVEL_ERROR_VERSION) {
            $body['errorCode'] = BinarySchema::TYPE_INT16;
        }

        $body['logDirs'] = ['logDir' => static::logDirClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a directory entry for the version of the api that this class unpacks
     *
     * @return class-string<DescribeLogDirsResponseLogDir>
     */
    protected static function logDirClass(): string
    {
        return static::VERSION >= self::VOLUME_SIZE_VERSION
            ? DescribeLogDirsResponseLogDir::class
            : DescribeLogDirsResponseLogDirV3::class;
    }
}
