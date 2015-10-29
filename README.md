# Phalcon - MS SQL Server (PDO) Adapter
- Phalcon 2.0.8 support
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
