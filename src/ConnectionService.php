<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\Adapters\PdoDbAdapter;
use Behance\NBD\Dbal\ConfigService;
use Behance\NBD\Dbal\Exceptions\ConfigMissingException;
use Behance\NBD\Dbal\Exceptions\ConnectionSupportException;

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
 *
 * [NEW]
 * - During connection retrieval, if table is specified, rules #2 is applied on a per-table basis
 *    - Ex, a master conection for table ABC would not force replica connection to default to master for table DEF
 * - If a master connection retrieval is made without a table specified, fallback to legacy behavior for future calls
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
   * @var string[] a list of any tables that have used the master connection
   */
  private $_master_tables = [];

  /**
   * Enabled to trigger fallback to table-agnostic connection behavior (legacy)
   *
   * @var bool
   */
  private $_master_flag = false;

  /**
   * @param Behance\NBD\Dbal\ConfigService $config
   */
  public function __construct( ConfigService $config ) {

    $this->_config = $config;

  } // __construct


  /**
   * @param string $table  [NEW] follow connection management rules PER-TABLE when supplied
   *
   * @return PDO
   */
  public function getMaster( $table = null ) {

    if ( !$this->_master_flag ) {

      if ( $table ) {
        // NOTE: using less-than-ideal array index to store table name, prevents searching a flat array before appending
        $this->_master_tables[ $table ] = true;
      }
      else {
        $this->_master_flag = true; // Enable fallback to Doctrine-compatible behavior
      }

    } // if !master_flag

    if ( !$this->_master ) {
      $this->_master = $this->_buildAdapter( $this->_config->getMaster() );
    }

    return $this->_master;

  } // getMaster


  /**
   * IMPORTANT: to write-then-read slave lag issues, when a master database
   * has already been selected, continue using it instead of creating a replica connection
   *
   * @param string|null $table  optionally specify which table this query is for
   *
   * @return PDO
   */
  public function getReplica( $table = null ) {

    if ( $this->isUsingMaster( $table ) ) {
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
   * Determines if at any point of the request, a master connection was in use.
   * When table is consistently used to retrieve connections, status is specific to table
   *
   * @param string $table
   *
   * @return bool
   */
  public function isUsingMaster( $table = null ) {

    return ( $table )
           ? $this->isTableUsingMaster( $table )
           : !empty( $this->_master );

  } // isUsingMaster


  /**
   * Determines if an individual table is flagged to require master for connection
   *
   * @param string $table
   *
   * @return bool
   */
  public function isTableUsingMaster( $table ) {

    return ( $this->_master_flag || isset( $this->_master_tables[ $table ] ) );

  } // isTableUsingMaster

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

    $this->_master_flag   = false;
    $this->_master_tables = [];

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

   /**
    * Add error/warning suppression to prevent double reporting
    * @see https://bugs.php.net/bug.php?id=73878
    */
    return @new \PDO( $dsn, $user, $pass, $options );

  } // _createConnection


} // ConnectionService
