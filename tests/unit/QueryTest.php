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
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
  ];

  private $_table    = 'my_table';

  /**
   * @test
   */
  public function crudOperations() {

    $adapter = $this->_getLiveAdapter();
    $now     = 'NOW()';
    $enabled = 1;
    $not_enabled = 0;

    $data    = [
        'enabled'     => $enabled,
        'created_on'  => new Sql( $now ),
        'modified_on' => new Sql( $now )
    ];

    $id = $adapter->insert( $this->_table, $data );

    $this->assertNotEmpty( $id );

    $inserted_row = $this->_fetchRow( $adapter, $id );

    $this->assertEquals( $id, $inserted_row['id'] ); // NOTE: lastInsertId is not type aware
    $this->assertNotEquals( $now, $inserted_row['created_on'] );
    $this->assertNotEquals( $now, $inserted_row['created_on'] );
    $this->assertSame( $enabled, $inserted_row['enabled'] ); // PDO automatically converts type with native prepare

    $updated = $adapter->update( $this->_table, [ 'enabled' => $not_enabled ], [ 'id' => $id ] );

    $this->assertSame( 1, $updated );

    $updated_row = $this->_fetchRow( $adapter, $id );

    $this->assertSame( $not_enabled, $updated_row['enabled'] );

    $deleted = $adapter->delete( $this->_table, [ 'id' => $id ] );

    $this->assertNotEmpty( $deleted );

    $deleted_row = $adapter->getRow( $this->_table, [ 'id' => $id ] );

    $this->assertEmpty( $deleted_row );

  } // crudOperations


  /**
   * @param Behance\NBD\Dbal\AdapterInterface
   * @param int $id
   *
   * @return array
   */
  protected function _fetchRow( AdapterInterface $adapter, $id ) {

    $row = $adapter->getRow( $this->_table, [ 'id' => $id ] );

    $this->assertInternalType( 'array', $row );

    return $row;

  } // _fetchRow

} // QueryTest
