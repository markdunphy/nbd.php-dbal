<?php

namespace Behance\NBD\Dbal\Services;

use Behance\NBD\Dbal\Services\ConfigService;
use Behance\NBD\Dbal\Factories\ZendConnectionFactory;
use Behance\NBD\Dbal\Exceptions\ConnectionSupportException;
use Behance\NBD\Dbal\Exceptions\ConfigMissingException;

/**
 * Provides connection-management capabilities for master-slave database pools, follows basic rules here:
 *
 * @see http://www.doctrine-project.org/api/dbal/2.0/class-Doctrine.DBAL.Connections.MasterSlaveConnection.html
 *
 * Important for the understanding of this connection should be how and when it picks the slave or master.
 * 1. Slave if master was never picked before
 * 2. Master picked when any write operation is performed, or unspecified ->query() operation
 * 3. If master was picked once during the lifetime of the connection it will always get picked afterwards.
 * 4. One slave connection is randomly picked ONCE during a request.
 */
class ConnectionService {

  /**
   * AT MOST: one of each will be populated during the lifetime of a request
   * @var Zend\Db\Adapter\Adapter
   */
  private $_master,
          $_replica;

  /**
   * @var Behance\NBD\Dbal\Services\ConfigService $config
   */
  private $_config;

  /**
   * @var Behance\NBD\Dbal\Interfaces\ConnectionFactoryInterface
   */
  private $_connection_factory;


  /**
   * @param Behance\NBD\Dbal\Services\ConfigService                $config
   * @param Behance\NBD\Dbal\Interfaces\ConnectionFactoryInterface $adapter
   */
  public function __construct( ConfigService $config, ConnectionFactoryInterface $adapter = null ) {

    $this->_config             = $config;
    $this->_connection_factory = $adapter;

  } // __construct


  /**
   * @return Behance\NBD\Dbal\Services\ConfigService
   */
  public function getConfig() {

    return $this->_config;

  } // getConfig


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
   * IMPORTANT: to write-then-read slave lag issues, when a master database
   * has already been selected, continue using it instead of creating a replica connection
   *
   * @return Zend\Db\Adapter\Adapter
   */
  public function getReplica() {

    if ( !empty( $this->_master ) ) {
      return $this->_master;
    }

    if ( empty( $this->_replica ) ) {

      $config = null;

      try {
        $config = $this->_config->getReplica();
      }

      // IMPORTANT: when there are no explicit replicas, use master connection instead
      catch( ConfigMissingException $e ) {
        return $this->getMaster();
      }

      $this->_replica = $this->_buildAdapter( $config );

    } // if !replica

    return $this->_replica;

  } // getReplica


  /**
   * @return array
   */
  public function getOpenedConnections() {

    $results = [];

    if ( $this->_master ) {
      $results[] = $this->_master;
    }

    if ( $this->_replica ) {
      $results[] = $this->_replica;
    }

    return $results;

  } // getOpenedConnections


  /**
   * @return int  number of connections closed
   */
  public function closeOpenedConnections() {

    $connections = $this->getOpenedConnections();

    foreach ( $connections as $connection ) {
      $connection->getDriver()->getConnection()->disconnect();
    }

    // Ensure objects are removed, since connections are terminated
    $this->_master  = null;
    $this->_replica = null;

    return count( $connections );

  } // closeOpenedConnections


  /**
   * @throws Behance\NBD\Dbal\Exceptions\ConnectionSupportException
   *
   * @return Behance\NBD\Dbal\Interfaces\ConnectionFactoryInterface
   */
  protected function _getConnectionFactory() {

    if ( !$this->_connection_factory ) {
      $this->_connection_factory = new ZendConnectionFactory();
    }

    return $this->_connection_factory;

  } // _getConnectionFactory

  /**
   * @param array $config
   *
   * @return mixed  adapter based on current type
   */
  private function _buildAdapter( array $config ) {

    return $this->_getConnectionFactory()->build( $config );

  } // _buildAdapter

} // ConnectionService
