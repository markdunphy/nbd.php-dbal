<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\ConfigService;
use Behance\NBD\Dbal\Test\BaseTest;

class FactoryTest extends BaseTest {

  private $_server_config = [
      'host' => 'abc.com',
      'port' => 12345
  ];

  /**
   * @test
   * @dataProvider configProvider
   */
  public function createConfig( $trial_config ) {

    $config = $this->getMock( ConfigService::class, [ 'addMaster', 'addReplica' ] );

    $using_master = (int)( !empty( $trial_config['master'] ) );

    $config->expects( $this->exactly( $using_master ) )
      ->method( 'addMaster' );

    $replica_count = ( empty( $trial_config['replicas'] ) )
                     ? 0
                     : count( $trial_config['replicas'] );

    $config->expects( $this->exactly( $replica_count ) )
      ->method( 'addReplica' );

    Factory::create( $trial_config, $config );

  } // createConfig


  /**
   * @test
   */
  public function configProvider() {

    $master_only = [
        'master' => $this->_server_config
    ];

    $replica_only = [
        'replicas' => [
            $this->_server_config,
            $this->_server_config
        ]
    ];

    return [
        'Empty'        => [ [] ],
        'Master-Only'  => [ $master_only ],
        'Replica-Only' => [ $replica_only ],
        'Both'         => [ array_merge( $master_only, $replica_only ) ]
    ];

  } // configProvider


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\ConfigRequirementException
   * @dataProvider badReplicaConfigProvider
   */
  public function badReplicaConfig( $bad_replica_config ) {

    $trial_config = [
        'replicas' => $bad_replica_config
    ];

    Factory::create( $trial_config );

  } // badReplicaConfig


  /**
   * @return array
   */
  public function badReplicaConfigProvider() {

    return [
        [ 'abc' ],
        [ 123 ],
        [ true ],
        [ ( function(){} ) ],
    ];

  } // badReplicaConfigProvider

} // FactoryTest
