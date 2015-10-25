<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\ConnectionService;
use Behance\NBD\Dbal\Exceptions\QueryRequirementException;

use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Intends to shield application from any connection management
 * or state during the query process
 */
interface AdapterInterface {

  const EVENT_CONNECTION_CONNECT    = 'db.connection.connect';
  const EVENT_CONNECTION_DISCONNECT = 'db.connection.disconnect';
  const EVENT_CONNECTION_RECONNECT  = 'db.connection.reconnect';

  const EVENT_QUERY_PRE_EXECUTE     = 'db.query.pre_execute';
  const EVENT_QUERY_POST_EXECUTE    = 'db.query.post_execute';


  /**
   * @param string $table
   * @param array  $data
   * @param array  $options   add 'ignore' => true to create INSERT IGNORE statement
   *
   * @return int|string  ID of generated row
   */
  public function insert( $table, array $data );


  /**
   * @param string $table
   * @param array  $data
   *
   * @return int|string|bool  ID of generated row, if one was added
   */
  public function insertIgnore( $table, array $data );


  /**
   * @param string $table        where to insert
   * @param array  $data         what to insert
   * @param array  $update_data  what to use for update statement in event of insert duplicate detection
   *
   * @return int|string|bool  ID of generated row, 1 if one was added, 2 if rows were updated
   */
  public function insertOnDuplicateUpdate( $table, array $data, array $update_data );


  /**
   * @param string  $table
   * @param array   $data
   * @param mixed   $where
   *
   * @return int  rows affected
   */
  public function update( $table, array $data, $where );


  /**
   * @param string $table
   * @param mixed  $where
   *
   * @return int  rows affected
   */
  public function delete( $table, $where );


  /**
   * @return bool
   */
  public function beginTransaction();


  /**
   * @return bool
   */
  public function commit();


  /**
   * @return bool
   */
  public function rollback();


  /**
   * Prepares and executes a raw SQL statement
   *
   * IMPORTANT: provided for the thinnest SQL compatibility possible,
   * default to helper methods to avoid writing raw SQL
   *
   * @param string $sql         can contain prepared statement placeholders ('?'' or ':key')
   * @param array  $parameters  format must match placeholders, if using ?, a flat array, otherwise key/value
   * @param bool   $use_master  flags to use a master or replica connection (subject to connection rules)
   *
   * @return PDOStatement  post-execution statement
   */
  public function query( $sql, array $parameters = null );


  /**
   * Prepares and executes a raw SQL statement
   *
   * IMPORTANT: provided for the thinnest SQL compatibility possible,
   * default to helper methods to avoid writing raw SQL
   *
   * @param string $sql         can contain prepared statement placeholders ('?'' or ':key')
   * @param array  $parameters  format must match placeholders, if using ?, a flat array, otherwise key/value
   * @param bool   $use_master  flags to use a master or replica connection (subject to connection rules)
   *
   * @return PDOStatement  post-execution statement
   */
  public function queryMaster( $sql, array $parameters = null );


  /**
   *
   *
   * @param string   $event_name
   * @param callable $handler
   */
  public function bindEvent( $event_name, callable $handler );


  /**
   * @param string $sql
   * @param array  $replacements
   * @param bool   $master        whether or not to use master connection
   *
   * @return mixed|null  data from first value returned in first row (if any), null otherwise
   */
  public function fetchOne( $sql, array $parameters = null, $master = false );


  /**
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master        whether or not to use master connection
   *
   * @return array  data from first row, empty array otherwise
   */
  public function fetchRow( $sql, array $parameters = null, $master = false );


  /**
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master
   *
   * @return array
   */
  public function fetchCol( $sql, array $parameters = null, $master = false );


  /**
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master
   *
   * @return array
   */
  public function fetchAll( $sql, array $parameters = null, $master = false );


  /**
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master
   *
   * @return array
   */
  public function fetchAssoc( $sql, array $parameters = null, $master = false );


  /**
   * @throws Behance\NBD\Dbal\Exceptions\QueryRequirementException when less than 2 columns are selected
   *
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master
   *
   * @return array
   */
  public function fetchPairs( $sql, array $parameters = null, $master = false );


  /**
   * Provided only for backwards compatibility, protects a value being entered
   * into an SQL statement from command escaping/injection
   *
   * @param mixed $value
   *
   * @return string
   */
  public function quote( $value );


  /**
   * @return Behance\NBD\Dbal\Services\ConnectionService
   */
  public function getConnection();


  /**
   * @return int  number of connections closed
   */
  public function closeConnection();

} // AdapterInterface
