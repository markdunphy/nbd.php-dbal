<?php

namespace Behance\Core\Dbal\Services;

use Behance\Core\Dbal\Test\BaseTest;
use Behance\Core\Dbal\Services\ConfigService;

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
   */
  public function construct() {

    $config  = new ConfigService();
    $connect = new ConnectionService( $config );

    $this->assertSame( $config, $connect->getConfig() );

  } // construct


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

    $connect = new ConnectionService( $config );

    $master = $connect->getMaster();

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

    $connect = new ConnectionService( $config );

    $replica = $connect->getReplica();

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
   * 1.
   */
  public function getReplicaMasterReplica() {

    $config = new ConfigService();
    $config->addReplica( $this->_db_config1 );
    $config->addMaster( $this->_db_config2 );

    $connect = new ConnectionService( $config );

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
   * 1. Replica is not available, but master is substituted in place
   * 2. Repeated access attempts will return same master instance
   * 3. Master and replica instance are one in the same
   * 4. Only single connection is reported as open
   */
  public function getReplicaFromMaster() {

    $config = new ConfigService();
    $config->addMaster( $this->_db_config1 ); // Only master configuration is added

    $connect = new ConnectionService( $config );

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

    $config  = new ConfigService();
    $connect = new ConnectionService( $config );

    // Ensure none to start
    $this->assertEquals( 0, $connect->closeOpenedConnections() );

    $config->addMaster( $this->_db_config1 );
    $config->addReplica( $this->_db_config2 );

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

    $config  = new ConfigService();
    $connect = new ConnectionService( $config );

    $config->addMaster( $this->_db_config1 );
    $config->addReplica( $this->_db_config2 );

    // Order here is important, otherwise master will be returned in both cases
    $connect->getReplica();

    $master  = $connect->getMaster();
    $replica = $connect->getReplica(); // Master override in effect

    $this->assertSame( $master, $replica );

    $this->assertEquals( 2, $connect->closeOpenedConnections() );
    $this->assertEquals( 0, $connect->closeOpenedConnections() );

  } // closeOpenedConnectionMasterOverride

} // ConnectionServiceTest
