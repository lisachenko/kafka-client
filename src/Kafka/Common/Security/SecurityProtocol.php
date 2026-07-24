<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Common\Security;

/**
 * Possible values for security.protocol configuration parameter
 */
final class SecurityProtocol
{
    public const string PLAINTEXT = 'PLAINTEXT';

    public const string SSL = 'SSL';

    public const string SASL_PLAINTEXT = 'SASL_PLAINTEXT';

    public const string SASL_SSL = 'SASL_SSL';
}
