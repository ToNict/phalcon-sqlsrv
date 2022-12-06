# Phalcon - MS SQL Server (PDO) Adapter

- Phalcon 5.0+ support
- PHP 8.1 support

```php
$di->set('db', function() use ($config) {
	return new \Phalcon\Db\Adapter\Pdo\Sqlsrv(array(
		"host"         => $config->database->host,
		"username"     => $config->database->username,
		"password"     => $config->database->password,
		"dbname"       => $config->database->name
	));
});

```
