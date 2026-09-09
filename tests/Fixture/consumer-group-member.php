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
 * A second member of a consumer group, in a process of its own.
 *
 * Two members of one group can not be driven from a single PHP process: the coordinator answers the JoinGroup of a
 * member only once every other member has rejoined, so a consumer that blocks in poll() keeps the other one from
 * ever getting there. This script is therefore started by
 * {@see \Protocol\Kafka\Tests\Integration\ConsumerGroupTest} with `proc_open()`, subscribes to the same topic in
 * the same group and reports what the rebalances give it, one JSON object per line on its standard output:
 *
 * ```
 * {"event":"assignment","partitions":{"t7-topic":[2]}}
 * {"event":"closed"}
 * ```
 *
 * It stops as soon as the file named by `stopFile` appears - which is how the test makes it leave the group - or
 * when its deadline is reached, so that a failing test can never leave a member behind.
 *
 * Usage: php tests/Fixture/consumer-group-member.php '<json options>'
 */

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;

require dirname(__DIR__, 2) . '/vendor/autoload.php';

$options = json_decode((string) ($argv[1] ?? ''), true, 512, JSON_THROW_ON_ERROR);

$topic    = (string) $options['topic'];
$stopFile = (string) $options['stopFile'];
$deadline = microtime(true) + (float) ($options['durationSeconds'] ?? 60.0);

$consumer = new KafkaConsumer([
    ClientConfig::BOOTSTRAP_SERVERS         => [(string) $options['bootstrapServer']],
    ClientConfig::CLIENT_ID                 => (string) ($options['clientId'] ?? 'kafka-client-t7-member'),
    ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
    ClientConfig::RETRY_BACKOFF_MS          => 250,
    ClientConfig::REQUEST_TIMEOUT_MS        => (int) ($options['requestTimeoutMs'] ?? 30000),

    ConsumerConfig::GROUP_ID                      => (string) $options['groupId'],
    ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => (string) ($options['strategy'] ?? 'range'),
    ConsumerConfig::SESSION_TIMEOUT_MS            => (int) ($options['sessionTimeoutMs'] ?? 6000),
    // The `rebalance_timeout` of the JoinGroup v1 request, which request.timeout.ms above has to exceed
    ConsumerConfig::MAX_POLL_INTERVAL_MS          => (int) ($options['maxPollIntervalMs'] ?? 10000),
    ConsumerConfig::HEARTBEAT_INTERVAL_MS         => (int) ($options['heartbeatIntervalMs'] ?? 1000),
    ConsumerConfig::FETCH_MAX_WAIT_MS             => 250,
    ConsumerConfig::AUTO_OFFSET_RESET             => OffsetResetStrategy::EARLIEST,
    ConsumerConfig::ENABLE_AUTO_COMMIT            => false,
]);

/**
 * Writes one event of this member on the standard output, as a single JSON line
 *
 * @param array<string, mixed> $event
 */
$report = static function (array $event): void {
    echo json_encode($event, JSON_THROW_ON_ERROR), PHP_EOL;
    flush();
};

$consumer->subscribe([$topic]);
$report(['event' => 'started']);

$lastAssignment = null;
while (microtime(true) < $deadline && !file_exists($stopFile)) {
    $consumer->poll(200);

    $assignment = [];
    foreach ($consumer->assignment() as $assignedTopic => $partitions) {
        $partitionIds = array_map(intval(...), array_values($partitions));
        sort($partitionIds);
        $assignment[$assignedTopic] = $partitionIds;
    }

    if ($assignment !== $lastAssignment) {
        $lastAssignment = $assignment;
        $report(['event' => 'assignment', 'partitions' => $assignment]);
    }
}

$consumer->close();
$report(['event' => 'closed']);
