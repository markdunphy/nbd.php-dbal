<?php

namespace Behance\NBD\Dbal\Abstracts;

use Behance\NBD\Dbal\Interfaces\DbAdapterInterface;
use Behance\NBD\Dbal\Services\ConnectionService;
use Behance\NBD\Dbal\Exceptions\QueryRequirementException;

use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Provides high-level implementation common to all DbAdapters
 */
abstract class DbAdapterAbstract implements DbAdapterInterface {

  /**
   * @var Behance\NBD\Dbal\Services\ConnectionService
   */
  protected $_connection;

  /**
   * @var Symfony\Component\EventDispatcher\EventDispatcherInterface
   */
  protected $_dispatcher;


  /**
   * @param Behance\NBD\Dbal\Services\ConnectionService $connection
   * @param Symfony\Component\EventDispatcher\EventDispatcherInterface
   */
  public function __construct( ConnectionService $connection, EventDispatcherInterface $event_dispatcher = null ) {

    $this->_connection = $connection;
    $this->_dispatcher = $event_dispatcher ?: new EventDispatcher();

  } // __construct


  /**
   * @param string $sql
   * @param array  $parameters
   *
   * @return Zend\Db\ResultInterface   TODO: replace with abstraction
   */
  abstract public function query( $sql, array $parameters = null );


  /**
   * @param string $sql
   * @param array  $parameters
   *
   * @return Zend\Db\ResultInterface   TODO: replace with abstraction
   */
  abstract public function queryMaster( $sql, array $parameters = null );


  /**
   * @param string   $event_name
   * @param callable $handler
   */
  public function bindEvent( $event_name, callable $handler ) {

    $this->_dispatcher->addListener( $event_name, $handler );

  } // bindEvent


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
    $row     = $zresult->current();
    $row     = ( empty( $row ) )
               ? []
               : array_values( $row );

    // IMPORTANT: no matter the result size, the return type is a single value
    return ( isset( $row[0] ) )
           ? $row[0]
           : null;

  } // fetchOne


  /**
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master        whether or not to use master connection
   *
   * @return array  data from first row, empty array otherwise
   */
  public function fetchRow( $sql, array $parameters = null, $master = false ) {

    $zresult = $this->_connectionQuery( $sql, $parameters, $master );
    $row     = $zresult->current();

    // IMPORTANT: no matter the result size, the return type is a single value
    return ( empty( $row ) )
           ? []
           : $row;

  } // fetchRow


  /**
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master
   *
   * @return array
   */
  public function fetchCol( $sql, array $parameters = null, $master = false ) {

    $zresult = $this->_connectionQuery( $sql, $parameters, $master );
    $results = [];

    foreach ( $zresult as $row ) {

      // TODO: use array_column in php 5.5+
      $row = array_values( $row );

      // IMPORTANT: first column value is added to result set ONLY
      if ( isset( $row[0] ) ) {
        $results[] = $row[0];
      }

    } // foreach zresult

    return $results;

  } // fetchCol


  /**
   * @param string $sql
   * @param array  $parameters
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
   * @param array  $parameters
   * @param bool   $master
   *
   * @return array
   */
  public function fetchAssoc( $sql, array $parameters = null, $master = false ) {

    $zresult = $this->_connectionQuery( $sql, $parameters, $master );
    $results = [];

    $current = $zresult->current();

    if ( empty( $current ) ) {
      return $results;
    }

    $zresult->rewind();

    foreach ( $zresult as $row ) {

      $values = array_values( array_slice( $row, 0, 1 ) );

      $results[ $values[ 0 ] ] = $row;

    } // foreach zresult

    return $results;

  } // fetchAssoc



  /**
   * @throws Behance\NBD\Dbal\Exceptions\QueryRequirementException when less than 2 columns are selected
   *
   * @param string $sql
   * @param array  $parameters
   * @param bool   $master
   *
   * @return array
   */
  public function fetchPairs( $sql, array $parameters = null, $master = false ) {

    $zresult = $this->_connectionQuery( $sql, $parameters, $master );
    $results = [];

    $current = $zresult->current();

    if ( empty( $current ) ) {
      return $results;
    }

    // Check just the first result, since resultsets are fixed width associative array
    if ( $zresult->getFieldCount() < 2 ) {
      throw new QueryRequirementException( "FetchPairs requires two columns to be selected" );
    }

    $zresult->rewind();

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
   * @return Behance\NBD\Dbal\Services\ConnectionService
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
   * @return Zend\Db\Adapter\Driver\ResultInterface|Zend\Db\ResultSet\ResultSetInterface
   */
  private function _connectionQuery( $sql, array $parameters = null, $master = false ) {

    return ( $master )
           ? $this->queryMaster( $sql, $parameters )
           : $this->query( $sql, $parameters );

  } // _connectionQuery

} // DbAdapterAbstract
