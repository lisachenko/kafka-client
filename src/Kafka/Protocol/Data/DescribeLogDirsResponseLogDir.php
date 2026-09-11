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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One log directory of a DescribeLogDirs answer, i.e. one entry of the `log_dirs` array
 *
 * <pre>
 *   DescribeLogDirsResponseLogDir => error_code log_dir [topics]
 *     error_code => INT16
 *     log_dir    => STRING
 *     topics     => DescribeLogDirsResponseTopic
 * </pre>
 *
 * `DESCRIBE_LOG_DIRS_RESPONSE_V0` in `DescribeLogDirsResponse.schemaVersions()` @ 1.1.1. The answer holds one entry
 * for **every** directory of `log.dirs` of the answering broker, whether or not the request named a replica that
 * lives in it: `ReplicaManager.describeLogDirs` maps over `config.logDirs` and reports each of them with the
 * absolute path that `new File(logDir).getAbsolutePath` produces.
 *
 * The error code is per directory, and `DescribeLogDirsResponse.LogDirInfo` @ 1.1.1 names the two values that can
 * appear in it:
 *
 * | Code | Name                | Meaning                                                                    |
 * |------|---------------------|----------------------------------------------------------------------------|
 * | 0    | None                | The directory is online; `topics` is what it holds of the requested replicas |
 * | 56   | KafkaStorageError   | The directory is configured but **offline**, and `topics` is empty          |
 * | -1   | Unknown             | Any other failure while the directory was read                             |
 *
 * A directory that is offline is therefore still reported, with the code 56 and no replica - which is how a client
 * tells "this disk holds nothing of yours" from "this disk is broken".
 *
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0)"
 */
class DescribeLogDirsResponseLogDir implements BinarySchemaInterface
{
    /**
     * Error code of this directory, 0 when it is online
     */
    public int $errorCode;

    /**
     * Absolute path of the log directory, as the broker resolved it from `log.dirs`
     */
    public string $logDir;

    /**
     * Topics that have a requested replica in this directory, indexed by the topic name
     *
     * @var array<string, DescribeLogDirsResponseTopic>
     */
    public array $topics;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode' => BinarySchema::TYPE_INT16,
            'logDir'    => BinarySchema::TYPE_STRING,
            'topics'    => ['topic' => DescribeLogDirsResponseTopic::class],
        ];
    }
}
