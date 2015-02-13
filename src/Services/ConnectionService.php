<?php

namespace Behance\Core\Dbal\Services;

use Behance\Core\Dbal\Services\ConfigService;
use Behance\Core\Dbal\Exceptions\ConnectionSupportException;

/**
 * Provides connection-management capabilities for master-slave database pools, follows basic rules here:
 *
 * @see http://www.doctrine-project.org/api/dbal/2.0/class-Doctrine.DBAL.Connections.MasterSlaveConnection.html
 *
 * Important for the understanding of this connection should be how and when it picks the slave or master.
 * 1. Slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used.
 * 2. Master picked when any write operation is performed, or unspecified ->query() operation
 * 3. If master was picked once during the lifetime of the connection it will always get picked afterwards.
 * 4. One slave connection is randomly picked ONCE during a request.
 */
class ConnectionService {

  /**
   * @var Zend\Db\Adapter\Adapter
   */
  private $_master,
          $_replica;

  /**
   * @var Behance\Core\Dbal\Services\ConfigService $config
   */
  private $_config;


  /**
   * @var Behance\Core\Dbal\Interfaces\ConnectionAdapterInterface
   */
  private $_connection_adapter;


  /**
   * @param string $type
   * @param Behance\Core\Dbal\Services\ConfigService $config
   */
  public function __construct( $type, ConfigService $config ) {

    $this->_type   = ucwords( strtolower( $type ) );
    $this->_config = $config;

  } // __construct


  /**
   * @return Zend\Db\Adapter\Adapter
   */
  public function getMaster() {

    if ( empty( $this->_master ) ) {
      $this->_master = $this->_buildAdapter( $this->_config->getMaster() );
    }

    return $this->_master;

  } // getMaster


  /**
   * @return Zend\Db\Adapter\Adapter
   */
  public function getReplica() {

    // IMPORTANT: if a master database has already been selected, continue to use it going forward
    if ( !empty( $this->_master ) ) {
      return $this->_master;
    }

    if ( empty( $this->_replica ) ) {
      $this->_replica = $this->_buildAdapter( $this->_config->getReplica() );
    }

    return $this->_replica;

  } // getReplica


  /**
   * @return array
   */
  public function getOpenedConnections() {

    $results;

    if ( $this->_master ) {
      $results[] = $this->_master;
    }

    if ( $this->_replica ) {
      $results[] = $this->_replica;
    }

    return $results;

  } // getOpenedConnections


  /**
   * @throws Behance\Core\Dbal\Exceptions\ConnectionSupportException
   *
   * @return Behance\Core\Dbal\Interfaces\ConnectionAdapterInterface
   */
  protected function _getConnectionAdapter() {

    if ( !$this->_connection_adapter ) {

      $class_name = '\\Behance\\Core\\Dbal\\Adapters\\Connection\\' . $this->_type . 'ConnectionAdapter';

      if ( !class_exists( $class_name ) ) {
        throw new ConnectionSupportException( "Connection adapter for {$this->_type} missing" );
      }

      $this->_connection_adapter = new $class_name();

    } // if !connection_adapter

    return $this->_connection_adapter;

  } // _getConnectionAdapter

  /**
   * @param array $config
   *
   * @return mixed  adapter based on current type
   */
  private function _buildAdapter( array $config ) {

    return $this->_getConnectionAdapter( $this->_type )->build( $config );

  } // _buildAdapter

} // ConnectionService
