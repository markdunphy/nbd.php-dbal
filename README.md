[![Build Status](https://travis-ci.org/behance/nbd.php-dbal.svg?branch=master)](https://travis-ci.org/behance/nbd.php-dbal)
[![Dependency Status](https://www.versioneye.com/user/projects/55240746971f7847ca0006e0/badge.svg?style=flat)](https://www.versioneye.com/user/projects/55240746971f7847ca0006e0)

# behance/nbd.php-dbal
Managed interface to database pools using ZF2 DB module

Provides connection management, with master-slave support
- Connection management follows basic pattern from Doctrine
@see http://www.doctrine-project.org/api/dbal/2.0/class-Doctrine.DBAL.Connections.MasterSlaveConnection.html

```
use Behance\NBD\Dbal\Factory;

$config['master'] = [
    'username' => 'admin',
    'password' => 'password',
    'host'     => 'db',
    'port'     => 3306,
    'database' => 'dbal_test',
    'driver'   => 'Pdo_Mysql'
];

$config['replicas'] = [
    [
        'username' => 'admin',
        'password' => 'password',
        'host'     => 'replica',
        'port'     => 3306,
        'database' => 'dbal_test',
        'driver'   => 'Pdo_Mysql'
    ],
    //[
    //    ...add as many slaves as necessary
    //]
];

$db = Factory::create( $config );

// Provides:
// insert, query, update, delete, transaction (beingTransaction, commit, rollback) methods


// @returns last inserted ID
$insert_id = $db->insert( 'table_name', [ 'key' => 'value' ] );

// @returns affected rows
$updated_rows = $db->update( 'table_name', [ 'key' => 'value' ], [ 'where_id' => 'where_id_value' ] );

// @returns affected rows
$deleted_rows = $db->delete( 'table_name', [ 'where_id' => 'where_id_value' ] );

// @returns array|bool
$resultset = $db->getRow( 'table_name', [ 'name' => 'Bob' ] );
```
