<?php

namespace Behance\NBD\Dbal\Adapters;

use Behance\NBD\Dbal\Abstracts\DbAdapterAbstract;

use Behance\NBD\Dbal\Exceptions\InvalidQueryException;
use Behance\NBD\Dbal\Exceptions\QueryException;
use Behance\NBD\Dbal\Exceptions\QueryRequirementException;

use Behance\NBD\Dbal\Events\QueryEvent;

use Zend\Db\Sql\Sql as ZendSql;
use Zend\Db\Sql\Where as ZendSqlWhere;
use Zend\Db\Sql\Predicate\PredicateInterface as ZendPredicate;

use Zend\Db\Adapter\Adapter as ZendAdapter;
use Zend\Db\Adapter\Exception\ExceptionInterface as ZendDbException;
use Zend\Db\Adapter\Exception\InvalidQueryException as ZendInvalidQueryException;
use Zend\Db\Adapter\Driver\StatementInterface;
use Zend\Db\Adapter\Driver\ResultInterface as ZendResultInterface;
use Zend\Db\ResultSet\ResultSet as ZendResultSet;

class ZendDbAdapter extends DbAdapterAbstract {

  // Unfortunately the only way to detect this issue is a string match
  const MESSAGE_SERVER_GONE_AWAY = 'server has gone away';


  /**
   * {@inheritDoc}
   */
  public function insert( $table, array $data ) {

    $adapter = $this->_getMasterAdapter();
    $sql     = $this->_getSqlBuilder( $adapter, $table );
    $insert  = $sql->insert();

    $insert->values( $data );

    $statement = $sql->prepareStatementForSqlObject( $insert );
    $result    = $this->_execute( $adapter, $statement );

    return $result->getGeneratedValue();

  } // insert


  /**
   * {@inheritDoc}
   */
  public function insertIgnore( $table, array $data ) {

    $adapter = $this->_getMasterAdapter();
    $sql     = $this->_getSqlBuilder( $adapter, $table );
    $insert  = $this->_getInsertIgnoreSqlBuilder();

    $insert->values( $data );

    $statement = $sql->prepareStatementForSqlObject( $insert );
    $result    = $this->_execute( $adapter, $statement );

    return $result->getGeneratedValue();

  } // insertIgnore


  /**
   * {@inheritDoc}
   */
  public function update( $table, array $data, $where ) {

    if ( empty( $data ) ) {
      throw new InvalidQueryException( 'No data provided for update' );
    }

    $this->_validateWhere( $where );

    $adapter = $this->_getMasterAdapter();
    $sql     = $this->_getSqlBuilder( $adapter, $table );
    $update  = $sql->update();

    $update->set( $data );
    $update->where( $where );

    $statement = $sql->prepareStatementForSqlObject( $update );
    $results   = $this->_execute( $adapter, $statement );

    return $results->getAffectedRows();

  } // update


  /**
   * {@inheritDoc}
   */
  public function delete( $table, $where ) {

    $this->_validateWhere( $where );

    // This must be a master connection...provide no option
    $adapter = $this->_getMasterAdapter();
    $sql     = $this->_getSqlBuilder( $adapter, $table );

    $delete  = $sql->delete();

    $delete->where( $where );

    $statement = $sql->prepareStatementForSqlObject( $delete );
    $results   = $this->_execute( $adapter, $statement );

    return $results->getAffectedRows();

  } // delete


  /**
   * {@inheritDoc}
   */
  public function query( $sql, array $parameters = null, ZendResultSet $resultset = null ) {

    $adapter   = $this->_getReplicaAdapter();
    $statement = $adapter->createStatement( $sql, $parameters );
    $result    = $this->_execute( $adapter, $statement );

    return $this->_processQueryResult( $result, $resultset );

  } // query


  /**
   * {@inheritDoc}
   */
  public function queryMaster( $sql, array $parameters = null, ZendResultSet $resultset = null ) {

    $adapter   = $this->_getMasterAdapter();
    $statement = $adapter->createStatement( $sql, $parameters );
    $result    = $this->_execute( $adapter, $statement );

    return $this->_processQueryResult( $result, $resultset );

  } // queryMaster


  /**
   * {@inheritDoc}
   */
  public function quote( $value ) {

    return $this->_getReplicaAdapter()->quoteValue( $value );

  } // quote


  /**
   * {@inheritDoc}
   */
  public function beginTransaction() {

    $adapter    = $this->_getMasterAdapter();
    $connection = $adapter->getDriver()->getConnection();

    return $connection->beginTransaction();

  } // beginTransaction


  /**
   * {@inheritDoc}
   */
  public function commit() {

    $adapter    = $this->_getMasterAdapter();
    $connection = $adapter->getDriver()->getConnection();

    return $connection->commit();

  } // commit


  /**
   * {@inheritDoc}
   */
  public function rollback() {

    $adapter    = $this->_getMasterAdapter();
    $connection = $adapter->getDriver()->getConnection();

    return $connection->rollback();

  } // rollback


  /**
   * @param Zend\Db\Adapter\Driver\StatementInterface $statement
   * @param int $retries  number of attempts executed on $statement, used to prevent infinite recursion
   *
   * @return Zend\Db\Adapter\Driver\ResultInterface|Zend\Db\ResultSet\ResultSetInterface
   */
  protected function _execute( ZendAdapter $adapter, StatementInterface $statement, $retries = 0 ) {

    $dispatcher = $this->_dispatcher;
    $exception  = null;
    $result     = null;

    // In lieu of performing this action in a finally block (forcing php 5.5+ only), use a closure
    $post_emit  = ( function( $result, $exception ) use ( $dispatcher, $statement ) {
      $dispatcher->dispatch( self::EVENT_QUERY_POST_EXECUTE, new QueryEvent( $statement, $result, $exception ) );
    } );

    // Fire pre-execute event
    $dispatcher->dispatch( self::EVENT_QUERY_PRE_EXECUTE, new QueryEvent( $statement ) );

    try {

      $result = $statement->execute();

      $post_emit( $result, $exception ); // Fire post-execute event

      return $result;

    } // try

    catch( ZendInvalidQueryException $e ) {

      $exception = new InvalidQueryException( $e->getMessage(), null, $e );

      $post_emit( $result, $exception ); // Fire post-execute event

      throw $exception;

    } // catch ZendInvalidQueryException

    catch( ZendDbException $e ) {

      $exception = new QueryException( $e->getMessage(), null, $e );

      $post_emit( $result, $exception ); // Fire post-execute event

      $recursion = ( $retries !== 0 );

      // Unfortunately, the only way to detect this specific issue (server gone away) is a string match
      if ( !$recursion && stripos( $e->getMessage(), self::MESSAGE_SERVER_GONE_AWAY ) !== false ) {

        $this->_reconnectAdapter( $adapter );

        ++$retries;

        // IMPORTANT: reattempt statement execution, using retry to prevent infinite recursion
        return $this->_execute( $adapter, $statement, $retries );

      } // if message = gone away

      throw $exception;

    } // catch ZendDbException

  } // _execute


  /**
   * @throws Behance\NBD\Dbal\Exceptions\QueryRequirementException  no WHERE provided
   * @throws Behance\NBD\Dbal\Exceptions\InvalidQueryException      unsupported format for WHERE
   *
   * @param array|string|PredicateInterface $where
   */
  protected function _validateWhere( $where ) {

    if ( empty( $where ) ) {
      throw new QueryRequirementException( 'WHERE is required' );
    }

    if ( is_numeric( $where ) ) {
      throw new InvalidQueryException( "Numeric WHERE statements not valid:" . var_export( $where, 1 ) );
    }

    $is_supported = ( is_string( $where ) || is_array( $where ) || $where instanceof ZendPredicate );

    if ( !$is_supported ) {
      throw new InvalidQueryException( "Unknown WHERE statement format: " . var_export( $where, 1 ) );
    }

  } // _validateWhere


  /**
   * @param Zend\Db\Adapter\Adapter $adapter
   * @param string $table
   *
   * @return Zend\Db\Sql\Sql
   */
  protected function _getSqlBuilder( ZendAdapter $adapter, $table = null ) {

    return new ZendSql( $adapter, $table );

  } // _getSqlBuilder


  /**
   * Reattempts a connection from an adapter that has gone stale
   *
   * @param Zend\Db\Adapter\Adapter $adapter
   */
  protected function _reconnectAdapter( ZendAdapter $adapter ) {

    $connection = $adapter->getDriver()->getConnection();

    $connection->disconnect();
    $connection->connect();

  } // _reconnectAdapter


  /**
   * @param Zend\Db\Adapter\Driver\ResultInterface $result
   * @param Zend\Db\ResultSet\ResultSetInterface   $resultset
   *
   * @return Zend\Db\Adapter\Driver\ResultInterface|Zend\Db\ResultSet\ResultSetInterface
   */
  protected function _processQueryResult( ZendResultInterface $result, ZendResultSet $resultset = null ) {

    if ( $result->isQueryResult() ) {

      $resultset = $resultset ?: new ZendResultSet();
      $resultset->initialize( $result );

      return $resultset;

    } // if isQueryResult

    return $result;

  } // _processQueryResult


  /**
   * Creates non-Zend SQL adapter for handling ignores, which are not natively supported
   *
   * @return Behance\NBD\Dbal\Adapters\Sql\InsertIgnore
   */
  protected function _getInsertIgnoreSqlBuilder() {

    return new Sql\InsertIgnore();

  } // _getInsertIgnoreSqlBuilder


} // ZendDbAdapter
