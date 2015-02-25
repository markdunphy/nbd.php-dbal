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
   * @param bool   $master    use the master connection
   *
   * @return Zend\Db\ResultInterface   TODO: replace with abstraction
   */
  abstract public function query( $sql, array $parameters = null, $master = true );


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

} // DbAdapterAbstract
