<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\Test\IntegrationTest;

/**
 *
 */
class QueryTest extends IntegrationTest {

  protected $_tables = [
      'my_table' => "CREATE TABLE `my_table` (
                      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                      `enabled` tinyint(4) unsigned NOT NULL,
                      `created_on` datetime DEFAULT NULL,
                      `modified_on` datetime DEFAULT NULL,
                      PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
      'secondary_table' => "CREATE TABLE `secondary_table` (
                      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                      `enabled` tinyint(4) unsigned NOT NULL,
                      `created_on` datetime DEFAULT NULL,
                      `modified_on` datetime DEFAULT NULL,
                      PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
  ];

  private $_table = 'my_table';

  private $_secondary_table = 'secondary_table';

  private $_enabled     = 1;
  private $_not_enabled = 0;
  private $_now         = 'NOW()';

  private $_adapter;

  /**
   * 1. Determines if test is currently running in integration mode, skips otherwise
   * 2. If necessary creations
   */
  protected function setUp() {

    parent::setUp();

    $this->_adapter = $this->_getLiveAdapter();

    $now  = 'NOW()';
    $data = [
        'enabled'     => $this->_enabled,
        'created_on'  => new Sql( $now ),
        'modified_on' => new Sql( $now )
    ];

    $this->_id = $this->_adapter->insert( $this->_table, $data );

  } // setUp

  /**
   * @test
   */
  public function crudOperations() {

    $enabled = $this->_enabled;
    $id      = $this->_id;
    $adapter = $this->_adapter;

    $this->assertNotEmpty( $id );

    $inserted_row = $this->_fetchRow( $adapter, $id );

    $this->assertEquals( $id, $inserted_row['id'] ); // NOTE: lastInsertId is not type aware
    $this->assertNotEquals( $this->_now, $inserted_row['created_on'] );
    $this->assertNotEquals( $this->_now, $inserted_row['created_on'] );
    $this->assertSame( $enabled, $inserted_row['enabled'] ); // PDO automatically converts type with native prepare

    // Ensure fetch/get commands are formatted correctly
    $this->assertEquals( $enabled, $adapter->getOne( $this->_table, 'enabled', [ 'id' => $id ] ) );
    $this->assertEquals( [ $enabled ], $adapter->getColumn( $this->_table, 'enabled', [ 'id' => $id ] ) );
    $this->assertEquals( $inserted_row, $adapter->getRow( $this->_table, [ 'id' => $id ] ) );
    $this->assertEquals( [ $inserted_row ], $adapter->getAll( $this->_table, [ 'id' => $id ] ) );
    $this->assertEquals( [ $id => $inserted_row ], $adapter->getAssoc( $this->_table, [ 'id' => $id ] ) );

    $updated = $adapter->update( $this->_table, [ 'enabled' => $this->_not_enabled ], [ 'id' => $id ] );

    $this->assertSame( 1, $updated );

    $updated_row = $this->_fetchRow( $adapter, $id );

    $this->assertSame( $this->_not_enabled, $updated_row['enabled'] );

    $deleted = $adapter->delete( $this->_table, [ 'id' => $id ] );

    $this->assertNotEmpty( $deleted );

    $deleted_row = $adapter->getRow( $this->_table, [ 'id' => $id ] );

    $this->assertEmpty( $deleted_row );

  } // crudOperations


  /**
   * @test
   */
  public function tableConnectionSegmentation() {

    $adapter    = $this->_getLiveReplicatedAdapter();
    $connection = $adapter->getConnection();

    $now  = 'NOW()';
    $data = [
        'enabled'     => 1,
        'created_on'  => new Sql( $now ),
        'modified_on' => new Sql( $now )
    ];

    $adapter->insert( $this->_table, $data );

    $this->assertTrue( $connection->isUsingMaster() );
    $this->assertTrue( $connection->isUsingMaster( $this->_table ) );
    $this->assertFalse( $connection->isUsingMaster( $this->_secondary_table ) );

    $adapter->closeConnection();

    $this->assertFalse( $connection->isUsingMaster() );
    $this->assertFalse( $connection->isUsingMaster( $this->_table ) );
    $this->assertFalse( $connection->isUsingMaster( $this->_secondary_table ) );

    $secondary_id = $adapter->insert( $this->_secondary_table, $data );

    $this->assertNotEmpty( $secondary_id );

    $this->assertTrue( $connection->isUsingMaster() );
    $this->assertFalse( $connection->isUsingMaster( $this->_table ) );
    $this->assertTrue( $connection->isUsingMaster( $this->_secondary_table ) );

  } // tableConnectionSegmentation


  /**
   * @param Behance\NBD\Dbal\AdapterInterface
   * @param int $id
   *
   * @return array
   */
  protected function _fetchRow( AdapterInterface $adapter, $id ) {

    $row = $adapter->fetchRow( "SELECT * FROM {$this->_table} WHERE `id` = ?", [ $id ] );

    $this->assertInternalType( 'array', $row );
    $this->assertEquals( $id, $row['id'] );

    return $row;

  } // _fetchRow

} // QueryTest
