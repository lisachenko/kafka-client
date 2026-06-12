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

final class SslProtocol
{
    public const string TLS = 'TLS';

    public const string TLSv1_1 = 'TLSv1_1';

    public const string TLSv1_2 = 'TLSv1_2';

    public const string SSL = 'SSL';

    public const string SSLv2 = 'SSLv2';

    public const string SSLv3 = 'SSLv3';
}
