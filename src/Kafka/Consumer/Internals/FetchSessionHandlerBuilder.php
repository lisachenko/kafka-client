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

namespace Protocol\Kafka\Consumer\Internals;

use Closure;
use LogicException;
use Protocol\Kafka\Common\TopicPartition;

/**
 * Collects the partitions of the next Fetch request of one session, the `FetchSessionHandler.Builder` of Java.
 *
 * The partitions are added in the order the request should ask for them - the order the broker fills a bounded
 * answer in - and {@see self::build()} hands the whole set to the {@see FetchSessionHandler} that opened the
 * builder, which works out what may be left out of the request and what the session has to forget.
 *
 * Building moves the state of the session on, so a builder is good for exactly one request: a second
 * {@see self::build()} throws, where the Java builder would fail with a `NullPointerException` on its emptied map.
 *
 * @see docs/protocol/2.8.md, section "Fetch sessions (v7, KIP-227)"
 */
final class FetchSessionHandlerBuilder
{
    /**
     * Partitions of the request that is being built, as topic => partition => fetch offset
     *
     * @var array<string, array<int, int>>|null
     */
    private ?array $next = [];

    /**
     * @param Closure(array<string, array<int, int>>): FetchRequestData $build What the handler makes of them
     */
    public function __construct(private readonly Closure $build) {}

    /**
     * Asks for the records of one partition, starting at the given offset
     *
     * A partition that is added twice keeps the place it had and takes the newer offset, exactly as the
     * `LinkedHashMap` of the Java builder does.
     *
     * The fetch offset may be the pair `[offset, currentLeaderEpoch]` that **Fetch v9** (Kafka 2.1, KIP-320) puts
     * on the wire; the session stores whichever shape it is given and compares it with the one it remembers, so a
     * partition whose *epoch* changed is sent again even when its offset did not.
     *
     * @param int|array{int, int} $fetchOffset
     */
    public function add(TopicPartition $topicPartition, int|array $fetchOffset): self
    {
        if ($this->next === null) {
            throw new LogicException('The request of this builder has already been built');
        }

        $this->next[$topicPartition->topic][$topicPartition->partition] = $fetchOffset;

        return $this;
    }

    /**
     * Works out the next request of the session and moves the session on
     */
    public function build(): FetchRequestData
    {
        if ($this->next === null) {
            throw new LogicException('The request of this builder has already been built');
        }

        $next       = $this->next;
        $this->next = null;

        return ($this->build)($next);
    }
}
