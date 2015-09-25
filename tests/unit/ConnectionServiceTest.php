<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\Test\BaseTest;
use Behance\NBD\Dbal\ConfigService;
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
    $connect = $this->getMock( ConnectionService::class, [ '_createConnection' ], [ $config ] );

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
    $connect = $this->getMock( ConnectionService::class, [ '_createConnection' ], [ $config ] );

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

    $config = new ConfigService();
    $config->addReplica( $this->_db_config1 );
    $config->addMaster( $this->_db_config2 );

    $connect  = $this->getMock( ConnectionService::class, [ '_createConnection' ], [ $config ] );
    $callback = ( function() {
        return new PPDO();
    } );

    $connect->expects( $this->exactly( 2 ) )
      ->method( '_createConnection' )
      ->will( $this->returnCallback( $callback ) );

    $replica = $connect->getReplica();
    $master  = $connect->getMaster();

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

    $connect = $this->getMock( ConnectionService::class, [ '_createConnection' ], [ $config ] );
    $callback = ( function() {
        return new PPDO();
    } );

    $connect->expects( $this->once() )
      ->method( '_createConnection' )
      ->will( $this->returnCallback( $callback ) );

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
   * @test
   * @dataProvider dbStateProvider
   */
  public function reconnect( $master ) {

    $pdo     = $this->_getDisabledMock( PPDO::class );
    $config  = $this->_getDisabledMock( ConfigService::class, [ 'getMaster', 'getSlave' ] );
    $connect = $this->getMock( ConnectionService::class, [ 'isUsingMaster', '_buildAdapter' ], [ $config ] );

    $config->expects( $this->any() )
      ->method( 'getMaster' )
      ->will( $this->returnValue( [] ) );

    $config->expects( $this->any() )
      ->method( 'getReplica' )
      ->will( $this->returnValue( [] ) );

    $connect->expects( $this->once() )
      ->method( '_buildAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $master_check = ( $master )
                    ? 1
                    : 2;

    $connect->expects( $this->exactly( $master_check ) )
      ->method( 'isUsingMaster' )
      ->will( $this->returnValue( $master ) );

    $this->assertSame( $pdo, $connect->reconnect() );

  } // reconnect


  /**
   * @return array
   */
  public function dbStateProvider() {

    return [
        'Master'   => [ true ],
        'Replica'  => [ false ],
    ];

  } // dbStateProvider


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

    $config   = new ConfigService();
    $config->addMaster( $this->_db_config1 );
    $config->addReplica( $this->_db_config2 );

    $connect  = $this->getMock( ConnectionService::class, [ '_createConnection' ], [ $config ] );
    $callback = ( function() {
        return new PPDO();
    } );

    $connect->expects( $this->any() )
      ->method( '_createConnection' )
      ->will( $this->returnCallback( $callback ) );

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

    $config   = new ConfigService();
    $config->addMaster( $this->_db_config1 );
    $config->addReplica( $this->_db_config2 );

    $connect  = $this->getMock( ConnectionService::class, [ '_createConnection' ], [ $config ] );
    $callback = ( function() {
        return new PPDO();
    } );

    $connect->expects( $this->any() )
      ->method( '_createConnection' )
      ->will( $this->returnCallback( $callback ) );

    // Order here is important, otherwise master will be returned in both cases
    $connect->getReplica();

    $master  = $connect->getMaster();
    $replica = $connect->getReplica(); // Master override in effect

    $this->assertSame( $master, $replica );

    $this->assertEquals( 2, $connect->closeOpenedConnections() );
    $this->assertEquals( 0, $connect->closeOpenedConnections() );

  } // closeOpenedConnectionMasterOverride

} // ConnectionServiceTest
