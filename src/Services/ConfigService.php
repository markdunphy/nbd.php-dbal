<?php

namespace Behance\NBD\Dbal\Services;

use Behance\NBD\Dbal\Exceptions\ConfigMissingException;
use Behance\NBD\Dbal\Exceptions\ConfigRequirementException;

/**
 * Protects encapsulation and protection for master-slave database configuration
 */
class ConfigService {

  /**
   * @var array holds configuration of the (single) master data store
   */
  private $_master;


  /**
   * @var array holds arrays of arrays of configuration for the replica data stores
   */
  private $_replicas;


  /**
   * @param array $config
   */
  public function addMaster( array $config ) {

    $this->_checkParameters( $config );

    $this->_master = $config;

  } // addMaster


  /**
   * @param array $config
   */
  public function addReplica( array $config ) {

    $this->_checkParameters( $config );

    $this->_replicas[] = $config;

  } // addReplica


  /**
   * @throws Behance\NBD\Dbal\Exceptions\ConfigMissingException
   *
   * @return array
   */
  public function getMaster() {

    if ( empty( $this->_master ) ) {
      throw new ConfigMissingException( "No configuration provided for master" );
    }

    return $this->_master;

  } // getMaster


  /**
   * Retrieves the configuration of a single replica, after random selection
   *
   * @return array
   */
  public function getReplica() {

    if ( empty( $this->_replicas ) ) {
      throw new ConfigMissingException( "No configuration provided replicas" );
    }

    $count    = count( $this->_replicas );
    $selected = rand( 0, $count - 1 );

    return $this->_replicas[ $selected ];

  } // getReplica


  /**
   * @throws Behance\NBD\Dbal\Exceptions\ConfigRequirementException
   *
   * @param array $config
   */
  private function _checkParameters( array $config ) {

    $required = [
        'username',
        'password',
        'host',
        'port',
        'database',
        'driver'
    ];

    $difference = array_diff( $required, array_keys( $config ) );

    if ( !empty( $difference ) ) {
      throw new ConfigRequirementException( "Missing: " . implode( ', ', $difference ) );
    }

  } // _checkParameters

} // ConfigService
