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

namespace Protocol\Kafka\Consumer;

/**
 * Enumeration of offset reset strategies
 */
final class OffsetResetStrategy
{
    /**
     * Fetch the earliest available offset
     */
    public const string EARLIEST = 'earliest';

    /**
     * Fetch the latest available offset for topic partition
     */
    public const string LATEST = 'latest';

    /**
     * Do not fetch offset and throw an exception if offset is not available
     */
    public const string NONE = 'none';
}
