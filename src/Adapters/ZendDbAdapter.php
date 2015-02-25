<?php

namespace Behance\Core\Dbal\Adapters;

use Behance\Core\Dbal\Abstracts\DbAdapterAbstract;

use Behance\Core\Dbal\Exceptions\InvalidQueryException;
use Behance\Core\Dbal\Exceptions\QueryException;
use Behance\Core\Dbal\Exceptions\QueryRequirementException;

use Behance\Core\Dbal\Events\QueryEvent;

use Zend\Db\Sql\Sql;
use Zend\Db\Sql\Where as SqlWhere;
use Zend\Db\Adapter\Exception\ExceptionInterface as ZendDbException;
use Zend\Db\Adapter\Exception\InvalidQueryException as ZendInvalidQueryException;
use Zend\Db\Adapter\Driver\StatementInterface;

class ZendDbAdapter extends DbAdapterAbstract {

  /**
   * {@inheritDoc}
   */
  public function insert( $table, array $data ) {

    $adapter = $this->_getMasterAdapter();
    $sql     = $this->_getSqlBuilder( $adapter, $table );
    $insert  = $sql->insert();

    $insert->values( $data );

    $statement = $sql->prepareStatementForSqlObject( $insert );
    $result    = $this->_execute( $statement );

    return $result->getGeneratedValue();

  } // insert


  /**
   * {@inheritDoc}
   */
  public function update( $table, array $data, $where ) {

    if ( empty( $where ) ) {
      throw new QueryRequirementException( 'Update requires WHERE' );
    }

    if ( empty( $data ) ) {
      throw new InvalidQueryException( 'No data provided for update' );
    }

    $adapter = $this->_getMasterAdapter();
    $sql     = $this->_getSqlBuilder( $adapter, $table );
    $update  = $sql->update();
    $where   = $this->_processWhere( $where );

    $update->set( $data );
    $update->where( $where );

    $statement = $sql->prepareStatementForSqlObject( $update );
    $results   = $this->_execute( $statement );

    return $results->getAffectedRows();

  } // update


  /**
   * {@inheritDoc}
   */
  public function delete( $table, $where ) {

    if ( empty( $where ) ) {
      throw new QueryRequirementException( 'Delete requires WHERE' );
    }

    // This must be a master connection...provide no option
    $adapter = $this->_getMasterAdapter();
    $sql     = $this->_getSqlBuilder( $adapter, $table );
    $delete  = $sql->delete();
    $where   = $this->_processWhere( $where );

    $delete->where( $where );

    $statement = $sql->prepareStatementForSqlObject( $delete );
    $results   = $this->_execute( $statement );

    return $results->getAffectedRows();

  } // delete


  /**
   * {@inheritDoc}
   */
  public function query( $sql, array $parameters = null, $master = true ) {

    $adapter   = ( $master )
                 ? $this->_getMasterAdapter()
                 : $this->_getReplicaAdapter();

    $statement = $adapter->createStatement( $sql, $parameters );

    return $this->_execute( $statement );

  } // query


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
   * @param Zend\Db\Adapter\Driver\StatementInterface
   *
   * @return Zend\Db\Adapter\Driver\ResultInterface
   */
  protected function _execute( StatementInterface $statement ) {

    $dispatcher = $this->_dispatcher;
    $exception  = null;
    $result     = null;

    $dispatcher->dispatch( self::EVENT_QUERY_PRE_EXECUTE, new QueryEvent( $statement ) );

    try {

      $result = $statement->execute();

      return $result;

    } // try

    catch( ZendInvalidQueryException $e ) {

      $exception = new InvalidQueryException( $e->getMessage(), null, $e );

      throw $exception;

    } // catch ZendInvalidQueryException

    catch( ZendDbException $e ) {

      $exception = new QueryException( $e->getMessage(), null, $e );

      throw $exception;

    } // catch ZendDbException

    finally {
      $dispatcher->dispatch( self::EVENT_QUERY_POST_EXECUTE, new QueryEvent( $statement, $result, $exception ) );
    }

  } // _execute


  /**
   * @param array|string|Zend\Db\Sql\Where $where
   *
   * @return Zend\Db\Sql\Where
   */
  protected function _processWhere( $where ) {

    if ( $where instanceof SqlWhere ) {
      return $where;
    }

    $sql_where = $this->_getSqlWhere();

    if ( is_array( $where ) ) {

      // TODO: handle case where array is keyed numerically (AND custom statements together)

      foreach ( $where as $key => $value ) {
        $sql_where->equalTo( $key, $value );
      }

    } // if is_array where

    else {
      throw new Exception( "WHERE string not yet supported" );
    }

    return $sql_where;

  } // _processWhere


  /**
   * @param Zend\Db\Adapter\Adapter $adapter
   * @param string $table
   *
   * @return Zend\Db\Sql\Sql
   */
  protected function _getSqlBuilder( $adapter, $table = null ) {

    return new Sql( $adapter, $table );

  } // _getSqlBuilder


  /**
   * @return Zend\Db\Sql\Where
   */
  protected function _getSqlWhere() {

    return new SqlWhere();

  } // _getSqlWhere

} // ZendDbAdapter
