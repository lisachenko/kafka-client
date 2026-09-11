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

namespace Protocol\Kafka\Tests\Unit\Fixture;

use Protocol\Kafka\Client;

/**
 * A {@see Client} whose clock and whose sleep are controlled by the test, so that the KIP-219 throttle wait can be
 * measured without a single real microsecond passing.
 *
 * The clock starts at a fixed moment and only moves when the test moves it ({@see self::advanceBy()}) or when the
 * client sleeps - a sleep here is exactly what a real `usleep()` would be: the clock jumps forward by the slept
 * time, and the microseconds are recorded for the assertions.
 *
 * @see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT
 * @see docs/protocol/2.8.md, section "Quotas and throttle time"
 */
final class ThrottleAwareTestClient extends Client
{
    /**
     * Arbitrary fixed moment the clock of this double starts at, as a UNIX timestamp with microseconds
     */
    private const float EPOCH = 1000000.0;

    /**
     * Every sleep this client performed, in microseconds, in the order they happened
     *
     * @var list<int>
     */
    private array $sleeps = [];

    /**
     * Seconds the clock of this double has moved on since {@see self::EPOCH}
     */
    private float $elapsedSeconds = 0.0;

    /**
     * Returns the microseconds of every sleep the client performed, in order
     *
     * @return list<int>
     */
    public function getSleeps(): array
    {
        return $this->sleeps;
    }

    /**
     * Moves the clock of this double forward, as if the caller had spent that time doing something else
     */
    public function advanceBy(float $seconds): void
    {
        $this->elapsedSeconds += $seconds;
    }

    /**
     * @inheritdoc
     */
    protected function currentTime(): float
    {
        return self::EPOCH + $this->elapsedSeconds;
    }

    /**
     * @inheritdoc
     */
    protected function sleepFor(int $microseconds): void
    {
        $this->sleeps[] = $microseconds;
        $this->advanceBy($microseconds / 1000000);
    }
}
