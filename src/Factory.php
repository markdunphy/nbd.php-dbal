<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\Services\ConfigService;
use Behance\NBD\Dbal\Services\ConnectionService;
use Behance\NBD\Dbal\Adapters\ZendDbAdapter;
use Behance\NBD\Dbal\Exceptions\ConfigRequirementException;

class Factory {

  /**
   * @throws Behance\NBD\Dbal\Exceptions\ConfigRequirementException if replicas in incorrect format
   *
   * @param array $config  contains a single configuration for key 'master'
   *                       contains an array of configurations for key 'replicas'
   * @param Behance\NB\Dbal\Services\ConfigService $config_service
   *
   * @return Behance\NBD\Dbal\Interfaces\DbAdapterInterface
   */
  public static function create( array $config, ConfigService $config_service = null ) {

    $config_service = $config_service ?: new ConfigService();

    if ( !empty( $config['master'] ) ) {
      $config_service->addMaster( $config['master'] );
    }

    if ( !empty( $config['replicas'] ) ) {

      if ( !is_array( $config['replicas'] ) ) {
        throw new ConfigRequirementException( "Replicas must be an array" );
      }

      foreach ( $config['replicas'] as $replica ) {
        $config_service->addReplica( $replica );
      }

    } // if replicas

    return new ZendDbAdapter( new ConnectionService( $config_service ) );

  } // create

} // Factory
