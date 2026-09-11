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

/**
 * One configuration entry of a DescribeConfigs answer of the versions 1 and 2, i.e. without the fields of KIP-569
 *
 * <pre>
 *   DescribeConfigsResponseConfigEntry (Version: 1 and 2) => config_name config_value read_only config_source
 *                                                            is_sensitive [config_synonyms]
 * </pre>
 *
 * Kafka 2.6 appended a `config_type` int8 and a `documentation` NULLABLE_STRING behind the synonyms (KIP-569); this
 * class only lowers the version constant that {@see DescribeConfigsResponseConfigEntry::getScheme()} follows, so
 * that an answer of a broker below 2.6 - or of a request this client sent with a lower version - is read without
 * them. Its {@see DescribeConfigsResponseConfigEntry::$configType} then keeps the
 * {@see \Protocol\Kafka\Admin\ConfigType::UNKNOWN} of the field's default and its documentation stays `null`.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v3)"
 */
final class DescribeConfigsResponseConfigEntryV1 extends DescribeConfigsResponseConfigEntry
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
