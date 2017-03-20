<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\ConfigService;
use Behance\NBD\Dbal\Test\BaseTest;
use Pseudo\Pdo as PPDO;

class ConnectionServiceTest extends BaseTest {

  private $_db_config1 = [
      'username' => 'abcd',
      'password' => '12345',
      'host'     => 'host1.com',
      'port'     => 3306,
      'database' => 'test',
      'driver'   => 'Pdo_Mysql'
  ];

  private $_db_config2 = [
      'username' => 'abcd',
      'password' => '12345',
      'host'     => 'host1.com',
      'port'     => 3306,
      'database' => 'test',
      'driver'   => 'Pdo_Mysql'
  ];


  /**
   * @test
   *
   * Check:
   * 1. Master connections are opened one time only
   * 2. Repeated retrievals use same instance
   * 3. Once master is selected, subsequent replica retrievals use master connection
   * 4. Once master is selected, replica is retrieved, only a single connection is open
   */
  public function getMaster() {

    $config = new ConfigService();
    $config->addMaster( $this->_db_config1 );

    $pdo     = new PPDO();
    $connect = $this->getMockBuilder( ConnectionService::class )
      ->setMethods( [ '_createConnection' ] )
      ->setConstructorArgs( [ $config ] )
      ->getMock();

    $connect->expects( $this->once() )
      ->method( '_createConnection' )
      ->with( $this->isType( 'string' ), $this->_db_config1['username'], $this->_db_config1['password'], $this->isType( 'array' ) )
      ->will( $this->returnValue( $pdo ) );

    $master = $connect->getMaster();

    $this->assertSame( $pdo, $connect->getMaster() );

    // Ensure repeated calls return same connection
    $this->assertSame( $master, $connect->getMaster() );

    // Ensure replica now returns MASTER connection
    $this->assertSame( $master, $connect->getReplica() );

    // Ensure only a single connection is open at this time
    $open_connections = $connect->getOpenedConnections();

    // This open connection should be master connection
    $this->assertCount( 1, $open_connections );
    $this->assertContains( $master, $open_connections );

  } // getMaster


  /**
   * @test
   *
   * Check:
   * 1. Replica connection is opened once only
   * 2. Repeated connection requests reuse instance
   * 3. Only single connection is reported as open
   */
  public function getReplica() {

    $config = new ConfigService();
    $config->addReplica( $this->_db_config2 );

    $pdo     = new PPDO();
    $connect = $this->getMockBuilder( ConnectionService::class )
      ->setMethods( [ '_createConnection' ] )
      ->setConstructorArgs( [ $config ] )
      ->getMock();

    $connect->expects( $this->once() )
      ->method( '_createConnection' )
      ->with( $this->isType( 'string' ), $this->_db_config1['username'], $this->_db_config1['password'], $this->isType( 'array' ) )
      ->will( $this->returnValue( $pdo ) );

    $replica = $connect->getReplica();

    $this->assertSame( $pdo, $connect->getReplica() );

    // Ensure repeated access reuses instance
    $this->assertSame( $replica, $connect->getReplica() );
    $this->assertSame( $replica, $connect->getReplica() );

    // Ensure only a single connection is open at this time
    $open_connections = $connect->getOpenedConnections();

    // This open connection should be master connection
    $this->assertCount( 1, $open_connections );
    $this->assertContains( $replica, $open_connections );

  } // getReplica


  /**
   * @test
   *
   * Check:
   * 1. Replica connection is retrieved first
   * 2. Master connection is retrieved next
   * 3. Subsequent replica connection retrievals return master connection instead
   * 4. 2 connections are currently open, but only the master is available for retrieval
   */
  public function getReplicaMasterReplica() {

    $config  = $this->_buildReadWriteConfig();
    $connect = $this->getMockBuilder( ConnectionService::class )
      ->setMethods( [ '_createConnection' ] )
      ->setConstructorArgs( [ $config ] )
      ->getMock();

    $callback = ( function() {
        return new PPDO();
    } );

    $connect->expects( $this->exactly( 2 ) )
      ->method( '_createConnection' )
      ->will( $this->returnCallback( $callback ) );

    $replica = $connect->getReplica();

    $master = $connect->getMaster();

    $this->assertNotSame( $replica, $master );

    // IMPORTANT: now that master has been selected, subsequent replica access MUST return master connection
    $this->assertSame( $master, $connect->getReplica() );

    // Open connections MUST contain both that were opened during lifetime
    $open_connections = $connect->getOpenedConnections();

    $this->assertCount( 2, $open_connections );
    $this->assertContains( $replica, $open_connections );
    $this->assertContains( $master, $open_connections );

  } // getReplicaMasterReplica


  /**
   * @test
   *
   * Check:
   * 1. Replica is not available (no config), but master is substituted in place
   * 2. Repeated access attempts will return same master instance
   * 3. Master and replica instance are one in the same
   * 4. Only single connection is reported as open
   */
  public function getReplicaFromMaster() {

    $config = new ConfigService();
    $config->addMaster( $this->_db_config1 ); // Only master configuration is added

    $connect = $this->_buildConnectionService( $config );

    // Master instance is created during replica access, since no replicas are available
    $master = $connect->getReplica();

    // Ensure repeated access reuses instance
    $this->assertSame( $master, $connect->getReplica() );
    $this->assertSame( $master, $connect->getMaster() );

    // Ensure only a single connection is open at this time
    $open_connections = $connect->getOpenedConnections();

    // This open connection should be master connection
    $this->assertCount( 1, $open_connections );
    $this->assertContains( $master, $open_connections );

  } // getReplicaFromMaster


  /**
   * Ensures that table-less queries will respect Doctrine-like connection management
   *
   * @test
   */
  public function isUsingMasterLegacyBehavior() {

    $config     = $this->_buildReadWriteConfig();
    $connection = $this->_buildConnectionService( $config );

    // Default case
    $this->assertFalse( $connection->isUsingMaster() );

    // After replica usage, still no master should be selected
    $connection->getReplica();
    $this->assertFalse( $connection->isUsingMaster() );

    $connection->getMaster(); // Should always be true past this point
    $this->assertTrue( $connection->isUsingMaster() );

    $connection->getReplica();
    $this->assertTrue( $connection->isUsingMaster() );

  } // isUsingMasterLegacyBehavior


  /**
   * Check that a specified table will report as master-in-use while others do not
   *
   * @test
   */
  public function isTableUsingMaster() {

    $table_write = 'write_me';
    $table_read  = 'read_me';

    $config     = $this->_buildReadWriteConfig();
    $connection = $this->_buildConnectionService( $config );

    $connection->getMaster( $table_write );

    $this->assertTrue( $connection->isUsingMaster( $table_write ) );
    $this->assertTrue( $connection->isTableUsingMaster( $table_write ) );

    // NOTE: when making an explicit call with another table, master requirements are segmented
    $this->assertFalse( $connection->isUsingMaster( $table_read ) );
    $this->assertFalse( $connection->isTableUsingMaster( $table_read ) );

    // IMPORTANT: connection retrievals without tables follow legacy behavior
    $this->assertTrue( $connection->isUsingMaster() );

  } // isTableUsingMaster


  /**
   * Check that using unspecified table will then report as master-in-use going forward
   *
   * @test
   */
  public function isTableUsingMasterFallback() {

    $table_write = 'write_me';
    $table_read  = 'read_me';

    $config     = $this->_buildReadWriteConfig();
    $connection = $this->_buildConnectionService( $config );

    $this->assertFalse( $connection->isUsingMaster() );

    $connection->getMaster(); // Retrieved without any table specified

    // Everything must report as using master going forward
    $this->assertTrue( $connection->isUsingMaster( $table_write ) );
    $this->assertTrue( $connection->isUsingMaster( $table_read ) );

    $this->assertTrue( $connection->isTableUsingMaster( $table_write ) );
    $this->assertTrue( $connection->isTableUsingMaster( $table_read ) );

  } // isTableUsingMasterFallback


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function isTableUsingMasterCounter( $use_table ) {

    $connection = $this->_getDisabledMock( ConnectionService::class, [ 'isTableUsingMaster' ] );
    $counter    = ( $use_table )
                  ? $this->once()
                  : $this->never();

    $connection->expects( $counter )
      ->method( 'isTableUsingMaster' );

    $params = ( $use_table )
              ? [ 'abc' ]
              : [];

    call_user_func_array( [ $connection, 'isUsingMaster' ], $params );

  } // isTableUsingMasterCounter


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function reconnect( $using_master ) {

    $result  = new PPDO();
    $connect = $this->_getDisabledMock( ConnectionService::class, [ 'isUsingMaster', 'closeOpenedConnections', 'getMaster', 'getReplica' ] );

    $connect->method( 'isUsingMaster' )
      ->will( $this->returnValue( $using_master ) );

    $connect->expects( $this->once() )
      ->method( 'closeOpenedConnections' );

    if ( $using_master ) {

      $connect->expects( $this->once() )
        ->method( 'getMaster' )
        ->will( $this->returnValue( $result ) );
      $connect->expects( $this->never() )
        ->method( 'getReplica' );

    } // if using_master
    else {

      $connect->expects( $this->never() )
        ->method( 'getMaster' );
      $connect->expects( $this->once() )
        ->method( 'getReplica' )
        ->will( $this->returnValue( $result ) );

    } // else (!using_master)

    $this->assertSame( $result, $connect->reconnect() );

  } // reconnect


  /**
   * @test
   */
  public function closeOpenedConnectionsEmpty() {

    $config  = new ConfigService();
    $connect = new ConnectionService( $config );

    // Ensure none to start
    $this->assertEquals( 0, $connect->closeOpenedConnections() );

  } // closeOpenedConnectionsEmpty


  /**
   * @test
   */
  public function closeOpenedConnections() {

    $config = new ConfigService();
    $config->addMaster( $this->_db_config1 );
    $config->addReplica( $this->_db_config2 );

    $connect = $this->_buildConnectionService( $config );

    $connect->getMaster();

    $this->assertEquals( 1, $connect->closeOpenedConnections() );

    // Ensure closed connections were no longer reported as open
    $this->assertEquals( 0, $connect->closeOpenedConnections() );

    // Order here is important, otherwise master will be returned in both cases
    $connect->getReplica();
    $connect->getMaster();

    $this->assertEquals( 2, $connect->closeOpenedConnections() );
    $this->assertEquals( 0, $connect->closeOpenedConnections() );

  } // closeOpenedConnections


  /**
   * @test
   */
  public function closeOpenedConnectionMasterOverride() {

    $config = new ConfigService();
    $config->addMaster( $this->_db_config1 );
    $config->addReplica( $this->_db_config2 );

    $connect = $this->_buildConnectionService( $config );

    // Order here is important, otherwise master will be returned in both cases
    $connect->getReplica();

    $master  = $connect->getMaster();
    $replica = $connect->getReplica(); // Master override in effect

    $this->assertSame( $master, $replica );

    $this->assertEquals( 2, $connect->closeOpenedConnections() );
    $this->assertEquals( 0, $connect->closeOpenedConnections() );

  } // closeOpenedConnectionMasterOverride


  /**
   * Check that legacy master usage will be reset after closing open connections
   *
   * @test
   */
  public function closeOpenedConnectionsLegacyMasterTable() {

    $table_write = 'write_me';
    $table_read  = 'read_me';

    $config     = $this->_buildReadWriteConfig();
    $connection = $this->_buildConnectionService( $config );

    $this->assertFalse( $connection->isUsingMaster() );

    $connection->getMaster(); // Retrieved without any table specified, flag legacy behavior

    $this->assertTrue( $connection->isUsingMaster( $table_write ) );
    $this->assertTrue( $connection->isUsingMaster( $table_read ) );

    $connection->closeOpenedConnections();

    $this->assertFalse( $connection->isUsingMaster() );
    $this->assertFalse( $connection->isUsingMaster( $table_write ) );
    $this->assertFalse( $connection->isUsingMaster( $table_read ) );

  } // closeOpenedConnectionsLegacyMasterTable


  /**
   * Check that table master usage will be reset after closing open connections
   *
   * @test
   */
  public function closeOpenedConnectionsMasterTable() {

    $table_write = 'write_me';
    $table_read  = 'read_me';

    $config     = $this->_buildReadWriteConfig();
    $connection = $this->_buildConnectionService( $config );

    $this->assertFalse( $connection->isUsingMaster() );
    $this->assertFalse( $connection->isUsingMaster( $table_write ) );

    $connection->getMaster( $table_write );

    $this->assertTrue( $connection->isUsingMaster( $table_write ) );
    $this->assertFalse( $connection->isUsingMaster( $table_read ) );

    $connection->closeOpenedConnections();

    $this->assertFalse( $connection->isUsingMaster() );
    $this->assertFalse( $connection->isUsingMaster( $table_write ) );
    $this->assertFalse( $connection->isUsingMaster( $table_read ) );

  } // closeOpenedConnectionsMasterTable


  /**
   * @return Behance\NBD\Dbal\ConnectionService
   */
  private function _buildConnectionService( ConfigService $config ) {

    $connect = $this->getMockBuilder( ConnectionService::class )
      ->setMethods( [ '_createConnection' ] )
      ->setConstructorArgs( [ $config ] )
      ->getMock();

    $callback = ( function() {
      return new PPDO();
    } );

    $connect->expects( $this->any() )
      ->method( '_createConnection' )
      ->will( $this->returnCallback( $callback ) );

    return $connect;

  } // _buildConnectionService


  /**
   * @return Behance\NBD\Dbal\ConfigService
   */
  private function _buildReadWriteConfig() {

    $config = new ConfigService();
    $config->addMaster( $this->_db_config1 );
    $config->addReplica( $this->_db_config2 );

    return $config;

  } // _buildReadWriteConfig

} // ConnectionServiceTest
