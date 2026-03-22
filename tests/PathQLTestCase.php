<?php

namespace MintyPHP\PathQL\Tests;

use PHPUnit\Framework\TestCase;
use MintyPHP\Core\DB;
use MintyPHP\Core\Cache;

abstract class PathQLTestCase extends TestCase
{
    /** @var DB|null */
    protected static $db;
    /** @var Cache|null */
    protected static $cache;

    public static function setUpBeforeClass(): void
    {
        $config = parse_ini_file("test_config.ini", true);
        if ($config === false || !isset($config['phpunit'])) {
            throw new \RuntimeException("Failed to parse config file");
        }
        $phpunitConfig = $config['phpunit'];
        if (!is_array($phpunitConfig)) {
            throw new \RuntimeException("Invalid config format");
        }

        $username = is_string($phpunitConfig['username'] ?? null) ? $phpunitConfig['username'] : '';
        $password = is_string($phpunitConfig['password'] ?? null) ? $phpunitConfig['password'] : '';
        $database = is_string($phpunitConfig['database'] ?? null) ? $phpunitConfig['database'] : '';
        $driver = is_string($phpunitConfig['driver'] ?? null) ? $phpunitConfig['driver'] : 'mysql';
        $address = is_string($phpunitConfig['address'] ?? null) ? $phpunitConfig['address'] : 'localhost';
        $port = is_string($phpunitConfig['port'] ?? null) ? $phpunitConfig['port'] : '3306';

        if ($driver !== 'mysql') {
            throw new \RuntimeException("Only MySQL is currently supported in MintyPHP");
        }

        // Create MintyPHP DB connection
        static::$db = new DB($address, $username, $password, $database, (int)$port, null);
        static::$cache = new Cache('pathql_test_', 'localhost:11211');

        // Begin transaction for test isolation
        static::$db->query('START TRANSACTION');
    }

    public static function tearDownAfterClass(): void
    {
        if (static::$db !== null) {
            static::$db->query('ROLLBACK');
        }
    }
}
