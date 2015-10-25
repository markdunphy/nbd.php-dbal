<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\ConfigService;
use Behance\NBD\Dbal\Exceptions\ConnectionSupportException;
use Behance\NBD\Dbal\Exceptions\ConfigMissingException;

use Behance\NBD\Dbal\Adapters\PdoDbAdapter;

/**
 * Provides connection-management capabilities for master-slave database pools, follows same basic rules as Doctrine:
 *
 * @see http://www.doctrine-project.org/api/dbal/2.0/class-Doctrine.DBAL.Connections.MasterSlaveConnection.html
 *
 * Important for the understanding of this connection should be how and when it picks the slave or master.
 * 1. Slave if master was never picked before
 * 2. Master picked when any write operation is performed
 * 3. If master was picked once during the lifetime of the connection it will always get picked afterwards
 * 4. One slave connection is randomly picked ONCE during a request
 */
class ConnectionService {

  const CHARSET_DEFAULT = 'utf8mb4';


  /**
   * AT MOST: one of each will be populated during the lifetime of a request
   * @var Behance\NBD\Dbal\AdapterInterface
   */
  private $_master,
          $_replica;

  /**
   * @var Behance\NBD\Dbal\ConfigService $config
   */
  private $_config;


  /**
   * @param Behance\NBD\Dbal\ConfigService $config
   */
  public function __construct( ConfigService $config ) {

    $this->_config = $config;

  } // __construct


  /**
   * @return PDO
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
   * @return PDO
   */
  public function getReplica() {

    if ( $this->isUsingMaster() ) {
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
   * Determines if at any point of the request, a master connection was in use
   *
   * @return bool
   */
  public function isUsingMaster() {

    return !empty( $this->_master );

  } // isUsingMaster


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
      $connection = null;
      unset( $connection ); // You can never be too sure.
    }

    // Ensure objects are removed, since connections are terminated
    $this->_master  = null;
    $this->_replica = null;

    return count( $connections );

  } // closeOpenedConnections


  /**
   * Closes all open connection and reopens a single connection
   * Maintains the current master/replica connection rules
   *
   * @return \PDO
   */
  public function reconnect() {

    // If master already exists, make sure it is repopulated instead of a replica
    $using_master = $this->isUsingMaster();

    $this->closeOpenedConnections();

    return ( $using_master )
           ? $this->getMaster()
           : $this->getReplica();

  } // reconnect


  /**
   * TODO: make $config into a object with protected accessors
   *
   * @param array $config
   *
   * @return \PDO
   */
  protected function _buildAdapter( array $config ) {

    $host    = $config['host'];
    $port    = $config['port'];
    $user    = $config['username'];
    $pass    = $config['password'];
    $db      = $config['database'];
    $charset = ( empty( $config['charset'] ) )
               ? self::CHARSET_DEFAULT
               : $config['charset'];

    $options = [
        \PDO::ATTR_ERRMODE            => \PDO::ERRMODE_EXCEPTION,
        \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
        \PDO::ATTR_EMULATE_PREPARES   => false,
    ];

    $dsn = "mysql:dbname={$db};host={$host};port={$port};charset={$charset}";

    return $this->_createConnection( $dsn, $user, $pass, $options );

  } // _buildAdapter


  /**
   * @param string $dns
   * @param string $user
   * @param string $pass
   * @param array  $options
   *
   * @return \PDO
   */
  protected function _createConnection( $dsn, $user, $pass, $options ) {

    return new \PDO( $dsn, $user, $pass, $options );

  } // _createConnection


} // ConnectionService
