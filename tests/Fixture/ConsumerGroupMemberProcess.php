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

namespace Protocol\Kafka\Tests\Fixture;

use RuntimeException;

/**
 * A second member of a consumer group, running in a process of its own.
 *
 * A group of two members can not be driven from one PHP process, because the coordinator holds the JoinGroup of a
 * member until every other member has rejoined: the consumer that is blocked in its poll() would never let the
 * other one send anything. This helper therefore starts `tests/Fixture/consumer-group-member.php` with
 * `proc_open()` and reads the JSON lines that member reports about itself, while the test drives its own consumer.
 *
 * @see \Protocol\Kafka\Tests\Integration\ConsumerGroupTest
 */
final class ConsumerGroupMemberProcess
{
    /**
     * How long to wait for the member to leave the group and exit after it was asked to stop, in seconds
     */
    private const float SHUTDOWN_TIMEOUT = 15.0;

    /**
     * The member process
     *
     * @var resource
     */
    private $process;

    /**
     * Standard input, output and error of the member process
     *
     * @var array<int, resource>
     */
    private array $pipes = [];

    /**
     * File whose existence asks the member to leave the group and exit
     */
    private readonly string $stopFile;

    /**
     * Part of a line that has been read but is not complete yet
     */
    private string $buffer = '';

    /**
     * Everything the member reported about itself, in order
     *
     * @var list<array<string, mixed>>
     */
    private array $events = [];

    /**
     * Everything the member wrote to its standard error, which is where a failure of it shows up
     */
    private string $errorOutput = '';

    /**
     * @param array<string, mixed> $options Options of the member, see tests/Fixture/consumer-group-member.php
     */
    public function __construct(array $options)
    {
        $this->stopFile = sys_get_temp_dir() . '/kafka-client-t7-stop-' . bin2hex(random_bytes(6));

        $command = sprintf(
            '%s %s %s',
            escapeshellarg(PHP_BINARY),
            escapeshellarg(__DIR__ . '/consumer-group-member.php'),
            escapeshellarg(json_encode($options + ['stopFile' => $this->stopFile], JSON_THROW_ON_ERROR))
        );

        $process = proc_open($command, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $this->pipes);
        if ($process === false) {
            throw new RuntimeException('Can not start the second member of the consumer group');
        }

        $this->process = $process;
        stream_set_blocking($this->pipes[1], false);
        stream_set_blocking($this->pipes[2], false);
    }

    public function __destruct()
    {
        $this->stop();
    }

    /**
     * Reads whatever the member reported since the last call
     */
    public function readEvents(): void
    {
        if (!isset($this->pipes[1])) {
            return;
        }

        $this->errorOutput .= (string) stream_get_contents($this->pipes[2]);
        $this->buffer      .= (string) stream_get_contents($this->pipes[1]);

        while (($newLine = strpos($this->buffer, "\n")) !== false) {
            $line         = substr($this->buffer, 0, $newLine);
            $this->buffer = substr($this->buffer, $newLine + 1);

            $event = json_decode(trim($line), true);
            if (is_array($event)) {
                $this->events[] = $event;
            }
        }
    }

    /**
     * Tells whether the member has subscribed to the topic and started its poll loop
     */
    public function hasStarted(): bool
    {
        return $this->hasReported('started');
    }

    /**
     * Tells whether the member left the group and its process ended
     */
    public function hasClosed(): bool
    {
        return $this->hasReported('closed');
    }

    /**
     * Returns the assignment the member reported last, null while it has not reported one
     *
     * @return array<string, list<int>>|null
     */
    public function getAssignment(): ?array
    {
        $assignment = null;
        foreach ($this->events as $event) {
            if (($event['event'] ?? '') === 'assignment') {
                /** @var array<string, list<int>> $assignment */
                $assignment = $event['partitions'] ?? [];
            }
        }

        return $assignment;
    }

    /**
     * Returns the partitions of one topic that the member holds, an empty list while it holds none
     *
     * @return list<int>
     */
    public function getPartitionsOf(string $topic): array
    {
        return $this->getAssignment()[$topic] ?? [];
    }

    /**
     * Asks the member to leave the group, without waiting for it
     */
    public function requestStop(): void
    {
        if (!file_exists($this->stopFile)) {
            touch($this->stopFile);
        }
    }

    /**
     * Asks the member to leave the group and waits for its process to end, killing it as a last resort
     */
    public function stop(): void
    {
        if (!is_resource($this->process)) {
            return;
        }

        $this->requestStop();

        $deadline = microtime(true) + self::SHUTDOWN_TIMEOUT;
        do {
            $this->readEvents();
            $status = proc_get_status($this->process);
            if (!$status['running']) {
                break;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        if ($status['running']) {
            proc_terminate($this->process, 9);
        }

        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];
        proc_close($this->process);

        @unlink($this->stopFile);
    }

    /**
     * Returns what the member wrote to its standard error, which is empty as long as nothing went wrong
     */
    public function getErrorOutput(): string
    {
        $this->readEvents();

        return $this->errorOutput;
    }

    /**
     * Tells whether the member reported the given event at least once
     */
    private function hasReported(string $event): bool
    {
        foreach ($this->events as $reported) {
            if (($reported['event'] ?? '') === $event) {
                return true;
            }
        }

        return false;
    }
}
