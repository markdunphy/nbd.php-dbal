<?php

namespace Behance\NBD\Dbal\Services;

use Behance\NBD\Dbal\Test\BaseTest;

class ConfigServiceTest extends BaseTest {

  private $_correct1 = [
      'username' => 'abcd',
      'password' => '12345',
      'host'     => 'host1.com',
      'port'     => 3306,
      'database' => 'test',
      'driver'   => 'Pdo_Mysql'
  ];

  private $_correct2 = [
      'username' => 'efgh',
      'password' => '67890',
      'host'     => 'host2.com',
      'port'     => 3307,
      'database' => 'test',
      'driver'   => 'Pdo_Mysql'
  ];


  private $_incorrect = [
      'abc' => 123,
      'def' => 456
  ];


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\ConfigRequirementException
   */
  public function badParamsMaster() {

    $config = new ConfigService();
    $config->addMaster( $this->_incorrect );

  } // badParamsMaster


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\ConfigRequirementException
   */
  public function badParamsReplica() {

    $config = new ConfigService();
    $config->addReplica( $this->_incorrect );

  } // badParamsReplica

  /**
   * @test
   */
  public function setGetMaster() {

    $config = new ConfigService();
    $config->addMaster( $this->_correct1 );
    $config->addReplica( $this->_correct2 );

    // Ensure first and subsequent calls return the same
    $this->assertSame( $this->_correct1, $config->getMaster() );
    $this->assertSame( $this->_correct1, $config->getMaster() );

  } // setGetMaster


  /**
   * @test
   */
  public function setGetReplica() {

    $config = new ConfigService();
    $config->addMaster( $this->_correct1 );
    $config->addReplica( $this->_correct2 );

    $this->assertSame( $this->_correct2, $config->getReplica() );

  } // setGetReplica


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\ConfigMissingException
   */
  public function setReplicaGetMaster() {

    $config = new ConfigService();
    $config->addReplica( $this->_correct1 );

    $config->getMaster();

  } // setReplicaGetMaster


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\ConfigMissingException
   */
  public function setMasterGetReplica() {

    $config = new ConfigService();
    $config->addMaster( $this->_correct1 );

    $config->getReplica();

  } // setMasterGetReplica


  /**
   * @test
   */
  public function setMasterReplicaGetMasterReplica() {

    $config = new ConfigService();
    $config->addMaster( $this->_correct1 );
    $config->addReplica( $this->_correct2 );

    $this->assertSame( $this->_correct1, $config->getMaster() );
    $this->assertSame( $this->_correct2, $config->getReplica() );

  } // setMasterReplicaGetMasterReplica

} // ConfigServiceTest
