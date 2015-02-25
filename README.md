# core-dbal
Managed interface to database pools using ZF2 DB module

Provides connection management, with master-slave support
- Connection management follows basic pattern from Doctrine
@see http://www.doctrine-project.org/api/dbal/2.0/class-Doctrine.DBAL.Connections.MasterSlaveConnection.html

```
use Behance\Core\Dbal\Services\ConfigService;
use Behance\Core\Dbal\Services\ConnectionService;
use Behance\Core\Dbal\Adapters\ZendDbAdapter;

$master = [
    'username' => 'admin',
    'password' => 'password',
    'host'     => 'db',
    'port'     => 3306,
    'database' => 'dbal_test',
    'driver'   => 'Pdo_Mysql'
];

$slave = [
    'username' => 'admin',
    'password' => 'password',
    'host'     => 'replica',
    'port'     => 3306,
    'database' => 'dbal_test',
    'driver'   => 'Pdo_Mysql'
];


$config = new ConfigService();

$config->addMaster( $master );
$config->addReplica( $slave );

$db = new ZendDbAdapter( new ConnectionService( $config ) );

// Provides:
// insert, query, update, delete, transaction (beingTransaction, commit, rollback) methods


// @returns last inserted ID
$insert_id = $db->insert( 'table_name', [ 'key' => 'value' ] );

// @returns affected rows
$updated_rows = $db->update( 'table_name', [ 'key' => 'value' ], [ 'where_id' => 'where_id_value' ] );

// @returns affected rows
$deleted_rows = $db->delete( 'table_name', [ 'where_id' => 'where_id_value' ] );

// @returns resultset (row iterable)
$resultset = $db->query( "SELECT * FROM 'table_name'" );
```
