# Phinx DB2 Adapter
An IBM DB2 database adapter for the Phinx migration package

### Installation
```sh
composer require wespals/phinx-db2-adapter
```

### Usage
```php
use Phinx\Db\Adapter\AdapterFactory;
use PhinxDb2Adapter\PhinxDb2Adapter;

AdapterFactory::instance()->registerAdapter('ibm', 'PhinxDb2Adapter');
```
