PHP Native Apache Kafka Client
==============================

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lisachenko/kafka-client/ci.yml?branch=main)
[![Code Coverage](https://img.shields.io/codecov/c/github/lisachenko/kafka-client/main)](https://app.codecov.io/gh/lisachenko/kafka-client)
[![Minimum PHP Version](http://img.shields.io/badge/php-%3E%3D%208.4-8892BF.svg)](https://www.php.net/supported-versions.php)
[![License](https://img.shields.io/packagist/l/lisachenko/kafka-client.svg)](https://packagist.org/packages/lisachenko/kafka-client)

`lisachenko/kafka-client` is a native, pure-PHP implementation of the Apache Kafka wire
protocol — no `ext-rdkafka` required. It ships a Producer, a Consumer and a low-level Admin
client, designed to stay close in spirit to the official Java client's API while feeling
natural in PHP.

Installation
------------

```bash
composer require lisachenko/kafka-client
```

Producer API
------------

The Producer API sends streams of records to topics in the Kafka cluster.

```php
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;

require __DIR__ . '/vendor/autoload.php';

$producer = new KafkaProducer([
    ProducerConfig::BOOTSTRAP_SERVERS => ['tcp://localhost'],
]);

$producer->send('test', new Record('foo'))->then(
    function (RecordMetadata $metadata): void {
        echo "Written to partition {$metadata->partition} at offset {$metadata->offset}\n";
    }
);
$producer->flush();
```

The only required option is `ProducerConfig::BOOTSTRAP_SERVERS`, a list of Kafka servers used
to bootstrap the cluster connection. For every other option, see the constants documented on
`Protocol\Kafka\Producer\ProducerConfig` and the [producer configuration] reference.

Consumer API
------------

The Consumer API reads streams of records from topics in the Kafka cluster.

```php
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;

$consumer = new KafkaConsumer([
    ConsumerConfig::BOOTSTRAP_SERVERS       => ['tcp://localhost'],
    ConsumerConfig::GROUP_ID                => 'kafka-daemon',
    ConsumerConfig::FETCH_MAX_WAIT_MS       => 5000,
    ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::LATEST,
    ConsumerConfig::SESSION_TIMEOUT_MS      => 30000,
    ConsumerConfig::AUTO_COMMIT_INTERVAL_MS => 10000,
    ConsumerConfig::METADATA_CACHE_FILE     => '/tmp/metadata.php',
]);

$consumer->subscribe(['test']);
for ($i = 0; $i < 100; $i++) {
    $records = $consumer->poll(1000);
    foreach ($records as $record) {
        echo json_encode($record), PHP_EOL;
    }
}
```

See the [consumer configuration] reference for the full set of options.

Admin API
---------

The Admin API exposes low-level cluster operations (group/topic metadata, offsets):

```php
use Protocol\Kafka\Admin\AdminClient;

$admin = new AdminClient(['bootstrap.servers' => ['tcp://localhost']]);
$groups = $admin->listGroups('tcp://localhost');
```

PHP-specific configuration
---------------------------

A few configuration options exist purely to make the client work well under PHP's
process-per-request model:

- `metadata.cache.file` — file used to cache cluster metadata; effectively cached by opcache in production.
- `stream.async.connect` — whether to connect to brokers asynchronously.
- `stream.persistent.connection` — whether to keep a persistent connection to the cluster.

For publishing from web requests, enabling persistent connections together with a metadata
cache file keeps producing as fast as possible.

Supported Kafka protocol versions
----------------------------------

`main` tracks the Kafka 0.11 wire protocol. Older, frozen protocol snapshots are kept on
dedicated branches for reference and are not actively developed further: `0.10.x` (Kafka
0.10.0), `0.9.x` (Kafka 0.9.0), `0.8.x` (Kafka 0.8.0).

Testing & Contributing
-----------------------

```bash
composer install
composer check   # coding standards + static analysis + PHPUnit
```

Issues and pull requests are welcome.

License
-------

Released under the [MIT license](LICENSE).

[producer configuration]: https://kafka.apache.org/documentation/#producerconfigs
[consumer configuration]: https://kafka.apache.org/documentation/#newconsumerconfigs
