<?php

namespace Behance\NBD\Dbal\Interfaces;

use Behance\NBD\Dbal\Services\ConnectionService;
use Behance\NBD\Dbal\Exceptions\QueryRequirementException;

use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Intends to shield application from any connection management
 * or state during the query process
 */
interface DbAdapterInterface {

  const EVENT_CONNECTION_CONNECT    = 'db.connection.connect';
  const EVENT_CONNECTION_DISCONNECT = 'db.connection.disconnect';
  const EVENT_CONNECTION_RECONNECT  = 'db.connection.reconnect';

  const EVENT_QUERY_PRE_EXECUTE     = 'db.query.pre_execute';
  const EVENT_QUERY_POST_EXECUTE    = 'db.query.post_execute';


  /**
   * Prepares and executes an SQL statement
   *
   * @param string $sql
   * @param array  $parameters
   *
   * @return Zend\Db\ResultInterface   TODO: replace with abstraction
   */
  public function query( $sql, array $parameters = null );


  /**
   * Prepares and executes an SQL statement against the master database
   *
   * @param string $sql
   * @param array  $parameters
   *
   * @return Zend\Db\ResultInterface   TODO: replace with abstraction
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
   * Provided only for backwards compatibility, protects a value being entered
   * into an SQL statement from command escaping/injection
   *
   * @param mixed $value
   *
   * @return string
   */
  public function quote( $value );


  /*
   *=========================================================================
   * ZF1 convenience method fillers -- Provided for backwards compatibility
   *=========================================================================
   * Executes prepared statements and slices/dices results based on method
   */


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
   * @param string $table
   * @param array  $data
   *
   * @return int  ID of generated row
   */
  public function insert( $table, array $data );


  /**
   * @param string $table
   * @param array  $data
   * @param array|string|Zend\Db\Sql\Where $where   TODO: replace with abstraction
   *
   * @return int  rows affected
   */
  public function update( $table, array $data, $where );


  /**
   * @param string $table
   * @param array|string|Zend\Db\Sql\Where $where   TODO: replace with abstraction
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
   * @return Behance\NBD\Dbal\Services\ConnectionService
   */
  public function getConnection();


  /**
   * @return int  number of connections closed
   */
  public function closeConnection();

} // DbAdapterInterface
