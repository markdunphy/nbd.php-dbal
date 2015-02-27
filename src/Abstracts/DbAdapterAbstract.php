<?php

namespace Behance\Core\Dbal\Abstracts;

use Behance\Core\Dbal\Services\ConnectionService;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Provides high-level implementation common to all DbAdapters
 */
abstract class DbAdapterAbstract {

  const EVENT_QUERY_PRE_EXECUTE  = 'db.query.pre_execute';
  const EVENT_QUERY_POST_EXECUTE = 'db.query.post_execute';

  /**
   * @var Behance\Core\Dbal\Services\ConnectionService
   */
  protected $_connection;

  /**
   * @var Symfony\Component\EventDispatcher\EventDispatcherInterface
   */
  protected $_dispatcher;


  /**
   * @param Behance\Core\Dbal\Services\ConnectionService $connection
   * @param Symfony\Component\EventDispatcher\EventDispatcherInterface
   */
  public function __construct( ConnectionService $connection, EventDispatcherInterface $event_dispatcher = null ) {

    $this->_connection = $connection;
    $this->_dispatcher = $event_dispatcher ?: new EventDispatcher();

  } // __construct


  /**
   * @param string $sql
   * @param array  $replacements
   *
   * @return Zend\Db\ResultInterface   TODO: replace with abstraction
   */
  abstract public function query( $sql, array $parameters = null );


  /**
   * @param string $sql
   * @param array  $replacements
   *
   * @return Zend\Db\ResultInterface   TODO: replace with abstraction
   */
  abstract public function queryMaster( $sql, array $parameters = null );


  /*
   *=========================================================================
   * ZF1 convenience method fillers -- Provided for backwards compatibility
   *=========================================================================
   */


  /**
   * @param string $sql
   * @param array  $replacements
   * @param bool   $master        whether or not to use master connection
   *
   * @return mixed|null  data from first value returned in first row (if any), null otherwise
   */
  public function fetchOne( $sql, array $parameters = null, $master = false ) {

    $zresult = $this->_connectionQuery( $sql, $parameters, $master );

    foreach ( $zresult as $row ) {
      $row = array_values( $row );

      // IMPORTANT: no matter the result size, the return type is a single value
      return $row[0];

    } // foreach zresult

    // Otherwise, there are no results, null is implicitly "returned"

  } // fetchOne


  /**
   * @param string $sql
   * @param array  $replacements
   * @param bool   $master
   *
   * @return array
   */
  public function fetchCol( $sql, array $parameters = null, $master = false ) {

    $zresult = $this->_connectionQuery( $sql, $parameters, $master );
    $results = [];

    foreach ( $zresult as $row ) {

      $row = array_values( $row );

      // IMPORTANT: first column value is added to result set ONLY
      $results[] = $row[0];

    } // foreach zresult

    return $results;

  } // fetchCol


  /**
   * @param string $sql
   * @param array  $params
   * @param bool   $master
   *
   * @return array
   */
  public function fetchAll( $sql, array $parameters = null, $master = false ) {

    $zresult = $this->_connectionQuery( $sql, $parameters, $master );
    $results  = [];

    foreach ( $zresult as $row ) {
      $results[] = $row;
    }

    return $results;

  } // fetchAll


  /**
   * @param string $sql
   * @param array  $params
   * @param bool   $master
   *
   * @return array
   */
  public function fetchPairs( $sql, array $parameters = null, $master = false ) {

    $zresult = $this->_connectionQuery( $sql, $parameters, $master );
    $results = [];

    foreach ( $zresult as $row ) {

      $row = array_values( $row );

      // IMPORTANT: no matter how many columns are returned, result set only uses two
      $results[ $row[0] ] = $row[1];

    } // foreach zresult

    return $results;

  } // fetchPairs


  /**
   * @param string $table
   * @param array  $data
   *
   * @return int  ID of generated row
   */
  abstract public function insert( $table, array $data );


  /**
   * @param string $table
   * @param array  $data
   * @param array|string|Zend\Db\Sql\Where $where   TODO: replace with abstraction
   *
   * @return int  rows affected
   */
  abstract public function update( $table, array $data, $where );


  /**
   * @param string $table
   * @param array|string|Zend\Db\Sql\Where $where   TODO: replace with abstraction
   *
   * @return int  rows affected
   */
  abstract public function delete( $table, $where );


  /**
   * @return bool
   */
  abstract public function beginTransaction();


  /**
   * @return bool
   */
  abstract public function commit();


  /**
   * @return bool
   */
  abstract public function rollback();


  /**
   * @return Behance\Core\Dbal\Services\ConnectionService
   */
  public function getConnection() {

    return $this->_connection;

  } // getConnection


  /**
   * @return int  number of connections closed
   */
  public function closeConnection() {

    return $this->_connection->closeOpenedConnections();

  } // closeConnection


  /**
   * @return Symfony\Component\EventDispatcher\EventDispatcherInterface
   */
  public function getEventDispatcher() {

    return $this->_dispatcher;

  } // getEventDispatcher


  /**
   * @return mixed depending on driver in use
   */
  protected function _getMasterAdapter() {

    return $this->_connection->getMaster();

  } // _getMasterAdapter


  /**
   * @return mixed depending on driver in use
   */
  protected function _getReplicaAdapter() {

    return $this->_connection->getReplica();

  } // _getReplicaAdapter


  /**
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master
   *
   * @return Zend\Db\ResultInterface
   */
  private function _connectionQuery( $sql, array $parameters = null, $master = false ) {

    return ( $master )
           ? $this->queryMaster( $sql, $parameters )
           : $this->query( $sql, $parameters );

  } // _connectionQuery

} // DbAdapterAbstract