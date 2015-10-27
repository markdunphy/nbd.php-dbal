[![Build Status](https://travis-ci.org/behance/nbd.php-dbal.svg?branch=master)](https://travis-ci.org/behance/nbd.php-dbal)
[![Dependency Status](https://www.versioneye.com/user/projects/55240746971f7847ca0006e0/badge.svg?style=flat)](https://www.versioneye.com/user/projects/55240746971f7847ca0006e0)

# behance/nbd.php-dbal
There are million of database adapters out there. But very few tick all (our very specific) boxes

### Goals
--- 

1. Very minimal dependencies, to be used in very diverse environments
2. Make every attempt to shield connection and management logic from implementer
3. Support master + many-slave replication patterns
4. Semi-intelligent connection choices:
    - Writes automatically choose master
    - Read queries randomly choose a single replica connection per request, unless...
    - Choosing a master connection at any point in the lifecycle will always use it going forward
    - Loosely follows Doctrine's tenets @see http://www.doctrine-project.org/api/dbal/2.0/class-Doctrine.DBAL.Connections.MasterSlaveConnection.html
5. Out-of-the-box convenience support for CRUD operations, accessors, and fallback to raw SQL (works with other SQL generators as well).
    - Automatic conversion to prepared statements for convenience parameters
7. Automatic retries for "mysql gone away" in long-running crons, workers, scripts
8. Provide deep introspection with events

### Usage
--- 

```
use Behance\NBD\Dbal;

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

$db = Dbal\Factory::create( $config );
```

### Testing
---   
Unit testing: 
1. `composer install`
2. `./vendor/bin/phpunit`

Integration testing: leveraging Docker, using actual mysql container
1. `docker-compose up -d`
2. `docker exec -it nbdphpdbal_web_1 /bin/bash`
3. `cd /app`
4. `./vendor/bin/phpunit`

### Operations
--- 
<table>

<tr>
<th>Operation</th>
<th>Example</th>
<th>Result</th>
<th>Notes</th>
</tr>

<tr>
<td>insert</td>
<td>$adapter->insert( 'table', [ 'key' => 'value' ] );</td>
<td>last insert ID</td>
<td></td>
</tr>

<tr>
<td>insertIgnore</td>
<td>$adapter->insertIgnore( 'table', [ 'key' => 'value' ] );</td>
<td>last insert ID, false if not inserted</td>
<td></td>
</tr>

<tr>
<td>insertOnDuplicateUpdate</td>
<td>$adapter->insertOnDuplicateUpdate( 'table', [ 'key' => 'value' ], [ 'created_on' => new Sql( 'NOW()' ) ] );</td>
<td>last insert ID, otherwise, 2 if updated</td>
<td>*see WHERE usage</td>
</tr>

<tr>
<td>update</td>
<td>$adapter->update( 'table', [ 'key' => 'new_value' ] );</td>
<td>rows affected</td>
<td>*see WHERE usage, enforces a non-empty WHERE is required</td>
</tr>

<tr>
<td>delete</td>
<td>$adapter->delete( 'table', [ 'id' => 12345 ] );</td>
<td>rows affected</td>
<td>*see WHERE usage, enforces a non-empty WHERE is required</td>
</tr>

<tr>
<td>beginTransaction</td>
<td>$adapter->beginTransaction();</td>
<td>bool successful</td>
<td>Nested transactions are not supported</td>
</tr>

<tr>
<td>commit</td>
<td>$adapter->commit();</td>
<td>bool successful</td>
<td></td>
</tr>

<tr>
<td>rollBack</td>
<td>$adapter->rollBack();</td>
<td>bool successful</td>
<td></td>
</tr>

<tr>
<td>query</td>
<td>$adapter->query( "SELECT * FROM `table` WHERE id=? AND enabled=?", [ 12345, 0 ] );</td>
<td>PDOStatement</td>
<td>*PDOStatement is already executed</td>
</tr>

<tr>
<td>queryMaster</td>
<td>$adapter->queryMaster( "SELECT * FROM `table` WHERE id=:id AND enabled=:enabled, [ ':id' => 12345, ':enabled' => 0 ] );</td>
<td>PDOStatement</td>
<td>*PDOStatement is already executed</td>
</tr>

<tr>
<td>queryMaster</td>
<td>$adapter->queryMaster( "SELECT * FROM `table` WHERE id=:id AND enabled=:enabled, [ ':id' => 12345, ':enabled' => 0 ] );</td>
<td>PDOStatement</td>
<td>*PDOStatement is already executed, connection is chosen to be master</td>
</tr>

<tr>
<td>quote</td>
<td>$adapter->queryMaster( "SELECT * FROM `table` WHERE id=:id AND enabled=:enabled, [ ':id' => 12345, ':enabled' => 0 ] );</td>
<td>string</td>
<td>Parameterized statements</td>
</tr>

</table>
