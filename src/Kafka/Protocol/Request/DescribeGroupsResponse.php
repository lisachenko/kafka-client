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
 * @date 28.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;

/**
 * Describe groups response
 */
class DescribeGroupsResponse extends AbstractResponse implements BinarySchemaInterface
{
    /**
     * List of groups as keys and group info as values
     *
     * @var array
     */
    public $groups = [];

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'groups' => ['groupId' => DescribeGroupResponseMetadata::class],
        ];
    }
}
