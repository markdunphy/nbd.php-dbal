<?php

namespace Behance\NBD\Dbal\Adapters;

use Behance\NBD\Dbal\AdapterAbstract;
use Behance\NBD\Dbal\Events\QueryEvent;
use Behance\NBD\Dbal\Exceptions;
use Behance\NBD\Dbal\Exceptions\QueryRequirementException;
use Behance\NBD\DbalException;

class PdoAdapter extends AdapterAbstract {

  // Unfortunately the only way to detect this issue is a string match
  const MESSAGE_SERVER_GONE_AWAY = 'server has gone away';

  /**
   * @var bool  tracks whether or not adapter is currently in transaction
   */
  private $_in_transaction = false;


  /**
   * {@inheritDoc}
   */
  public function insert( $table, array $data, array $options = null ) {

    $positions      = []; // SQL positional values (:key => value)
    $quoted_columns = $this->_quoteColumns( array_keys( $data ) );
    $quoted_table   = $this->_quoteTable( $table );
    $action         = ( empty( $options['ignore'] ) )
                      ? 'INSERT INTO'
                      : 'INSERT IGNORE INTO';

    list( $data, $positions ) = $this->_prepPositionalValues( $data );

    $columns_sql   = implode( ', ', $quoted_columns );
    $positions_sql = implode( ', ', $positions );

    $sql = sprintf( "%s %s (%s) VALUES (%s)", $action, $quoted_table, $columns_sql, $positions_sql );

    $on_duplicate = !empty( $options['on_duplicate'] );

    if ( $on_duplicate ) {

      if ( !$this->_isAssociativeArray( $options['on_duplicate'] ) ) {
          throw new Exceptions\InvalidQueryException( "Duplicate Key clause must be associative array" );
      }

      $update_values = [];

      foreach ( $options['on_duplicate'] as $column => $value ) {

        if ( is_object( $value ) && !$this->_isSqlObject( $value ) ) {
          throw new Exceptions\InvalidQueryException( "Object cannot be converted to string" );
        }

        $update_values[] = sprintf( '%s = %s', $this->_quoteColumn( $column ), $value );

      } // foreach on duplicate options

      $sql .= sprintf( ' ON DUPLICATE KEY UPDATE %s', implode( ', ', $update_values ) );

    } // if on_duplicate

    $flat_data = array_values( $data );
    $statement = $this->_executeMaster( $table, $sql, $flat_data );
    $adapter   = $this->_getMasterAdapter( $table );
    $last_id   = $adapter->lastInsertId();

    /**
     * IMPORTANT: PDO returns lastInsertId as a string.
     * For tables in which the primary ID is a non-integer type, composite key,
     * when using INSERT IGNORE or when using ON DUPLICATE KEY UPDATE,
     * the value of lastInsertId will be returned as string zero: "0"
     *
     * In those cases, it is preferable to return the affected-row count
     */
    if ( empty( $last_id ) ) {
      return $statement->rowCount();
    }

    return $last_id;

  } // insert


  /**
   * {@inheritDoc}
   */
  public function insertIgnore( $table, array $data ) {

    return $this->insert( $table, $data, [ 'ignore' => true ] );

  } // insertIgnore


  /**
   * {@inheritDoc}
   */
  public function insertOnDuplicateUpdate( $table, array $data, array $update_data ) {

    if ( empty( $update_data ) ) {
      throw new Exceptions\QueryRequirementException( "On duplicate update data is required" );
    }

    return $this->insert( $table, $data, [ 'on_duplicate' => $update_data ] );

  } // insertOnDuplicateUpdate


  /**
   * {@inheritDoc}
   */
  public function fetchOne( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    return $this->extractOneValue( $statement );

  } // fetchOne


  /**
   * {@inheritDoc}
   */
  public function fetchRow( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    return $this->extractKeyValues( $statement );

  } // fetchRow


  /**
   * {@inheritDoc}
   */
  public function fetchColumn( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    return $this->extractArrayValues( $statement );

  } // fetchColumn


  /**
   * {@inheritDoc}
   */
  public function fetchAll( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    return $this->extractAssocValues( $statement );

  } // fetchAll


  /**
   * {@inheritDoc}
   */
  public function fetchAssoc( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    return $this->extractColumnAssocValues( $statement );

  } // fetchAssoc


  /**
   * {@inheritDoc}
   */
  public function fetchPairs( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    return $this->extractPairValues( $statement );

  } // fetchPairs


  /**
   * {@inheritDoc}
   */
  public function getOne( $table, $column, $where, $master = false ) {

    list( $sql, $parameters ) = $this->prepSelectorQuery( $table, $column, $where );

    $statement = $this->queryTable( $table, $sql, $parameters, $master );

    return $this->extractOneValue( $statement );

  } // getOne


  /**
   * {@inheritDoc}
   */
  public function getRow( $table, $where = '', $master = false ) {

    list( $sql, $parameters ) = $this->prepStarQuery( $table, $where );

    $statement = $this->queryTable( $table, $sql, $parameters, $master );

    return $this->extractKeyValues( $statement );

  } // getRow


  /**
   * {@inheritDoc}
   */
  public function getColumn( $table, $column, $where, $master = false ) {

    list( $sql, $parameters ) = $this->prepSelectorQuery( $table, $column, $where );

    $statement = $this->queryTable( $table, $sql, $parameters, $master );

    return $this->extractArrayValues( $statement );

  } // getColumn


  /**
   * {@inheritDoc}
   */
  public function getAll( $table, $where, $master = false ) {

    list( $sql, $parameters ) = $this->prepStarQuery( $table, $where );

    $statement = $this->queryTable( $table, $sql, $parameters, $master );

    return $this->extractAssocValues( $statement );

  } // getAll


  /**
   * {@inheritDoc}
   */
  public function getAssoc( $table, $where, $master = false ) {

    list( $sql, $parameters ) = $this->prepStarQuery( $table, $where );

    $statement = $this->queryTable( $table, $sql, $parameters, $master );

    return $this->extractColumnAssocValues( $statement );

  } // getAssoc


  /**
   * {@inheritDoc}
   */
  public function update( $table, array $data, $where ) {

    if ( empty( $where ) ) {
      throw new Exceptions\QueryRequirementException( "WHERE required for update" );
    }

    if ( empty( $data ) ) {
      throw new Exceptions\InvalidQueryException( "No data for update" );
    }

    list( $data, $set ) = $this->_prepPositionValuePairs( $data );

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );

    $prepared = array_values( $data );

    if ( !empty( $where_prepared ) ) {
      $prepared = array_merge( $prepared, $where_prepared );
    }

    $quoted_table = $this->_quoteTable( $table );
    $sql          = sprintf( "UPDATE %s SET %s %s", $quoted_table, implode( ', ', $set ), $where_sql );

    return $this->_executeMaster( $table, $sql, $prepared )->rowCount();

  } // update


  /**
   * {@inheritDoc}
   */
  public function delete( $table, $where ) {

    if ( empty( $where ) ) {
      throw new Exceptions\QueryRequirementException( "WHERE required for delete" );
    }

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );

    $quoted_table = $this->_quoteTable( $table );
    $sql          = sprintf( "DELETE FROM %s %s", $quoted_table, $where_sql );

    return $this->_executeMaster( $table, $sql, $where_prepared )->rowCount();

  } // delete


  /**
   * {@inheritDoc}
   */
  public function beginTransaction() {

    $this->_in_transaction = true;

    return $this->_getMasterAdapter()->beginTransaction();

  } // beginTransaction


  /**
   * {@inheritDoc}
   */
  public function commit() {

    $this->_in_transaction = false;

    return $this->_getMasterAdapter()->commit();

  } // commit


  /**
   * {@inheritDoc}
   */
  public function rollBack() {

    $this->_in_transaction = false;

    return $this->_getMasterAdapter()->rollBack();

  } // rollBack


  /**
   * {@inheritDoc}
   */
  public function quote( $value, $type = \PDO::PARAM_STR ) {

    return $this->_getReplicaAdapter()->quote( $value, $type );

  } // quote


  /**
   * @return bool
   */
  public function isInTransaction() {

    return $this->_in_transaction;

  } // isInTransaction


  /**
   * {@inheritDoc}
   */
  public function closeConnection() {

    // IMPORTANT: reset this status
    $this->_in_transaction = false;

    return parent::closeConnection();

  } // closeConnection


  /**
   * Convenience wrapper for building SELECT * FROM ... SQL with parameters
   *
   * @param string            $table
   * @param string|array|null $where
   *
   * @return array  [ 0 => string: SQL, 1 => array: positional parameters ]
   */
  public function prepStarQuery( $table, $where ) {

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );

    $sql = sprintf( "SELECT * FROM %s %s", $this->_quoteTable( $table ), $where_sql );

    $parameters = ( empty( $where_prepared ) )
                  ? null
                  : $where_prepared;

    return [ $sql, $parameters ];

  } // prepStarQuery


  /**
   * Convenience wrapper for building SELECT * FROM ... SQL with parameters
   *
   * @param string            $table
   * @param string            $field
   * @param string|array|null $where
   *
   * @return array  [ 0 => string: SQL, 1 => array: positional parameters ]
   */
  public function prepSelectorQuery( $table, $field, $where ) {

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );

    $sql = sprintf( "SELECT %s FROM %s %s", $this->_quoteColumn( $field ), $this->_quoteTable( $table ), $where_sql );

    $parameters = ( empty( $where_prepared ) )
                  ? null
                  : $where_prepared;

    return [ $sql, $parameters ];

  } // prepSelectorQuery


  /**
   * NOTE: use Traversable as a stand-in for typehinting that does not work properly for testing mocks
   *
   * @param PDOStatement $statement
   *
   * @return mixed
   */
  public function extractOneValue( \Traversable $statement ) {

    if ( $statement->columnCount() === 0 ) {
      return;
    }

    // @see http://php.net/manual/en/pdostatement.fetchcolumn.php
    // Cannot use fetchcolumn if boolean values will fail row check
    $fetched = $statement->fetch( \PDO::FETCH_NUM );

    // IMPORTANT: no matter the result size, the return type is only ever the first column
    return ( $fetched === false || !isset( $fetched[0] ) )
           ? null
           : $fetched[0];

  } // extractOneValue


  /**
   * NOTE: use Traversable as a stand-in for typehinting that does not work properly for testing mocks
   *
   * @param PDOStatement $statement
   *
   * @return array
   */
  public function extractKeyValues( \Traversable $statement ) {

    // This is 0 on an empty result set
    if ( $statement->columnCount() === 0 ) {
      return [];
    }

    $row = $statement->fetch( \PDO::FETCH_ASSOC );

    // IMPORTANT: no matter the result size, the return type is the first row
    return ( empty( $row ) )
           ? []
           : $row;

  } // extractKeyValues


  /**
   * NOTE: use Traversable as a stand-in for typehinting that does not work properly for testing mocks
   *
   * @param PDOStatement $statement
   *
   * @return array
   */
  public function extractArrayValues( \Traversable $statement ) {

    return ( $statement->columnCount() === 0 )
           ? []
           : $statement->fetchAll( \PDO::FETCH_COLUMN );

  } // extractArrayValues


  /**
   * NOTE: use Traversable as a stand-in for typehinting that does not work properly for testing mocks
   *
   * @param PDOStatement $statement
   *
   * @return array
   */
  public function extractAssocValues( \Traversable $statement ) {

    return ( $statement->columnCount() === 0 )
           ? []
           : $statement->fetchAll( \PDO::FETCH_ASSOC );

  } // extractAssocValues


  /**
   * NOTE: use Traversable as a stand-in for typehinting that does not work properly for testing mocks
   *
   * @param PDOStatement $statement
   *
   * @return array
   */
  public function extractColumnAssocValues( \Traversable $statement ) {

    $results = [];

    while ( $row = $statement->fetch( \PDO::FETCH_ASSOC ) ) {

      // Retrieves the first associative value from the array
      $values = array_values( array_slice( $row, 0, 1 ) );

      $results[ $values[ 0 ] ] = $row;

    } // while fetch

    return $results;

  } // extractColumnAssocValues


  /**
   * NOTE: use Traversable as a stand-in for typehinting that does not work properly for testing mocks
   *
   * @param PDOStatement $statement
   *
   * @return array
   */
  public function extractPairValues( \Traversable $statement ) {

    // Columns will be 0 on an empty result set, which is not a violation
    if ( $statement->columnCount() === 0 ) {
      return [];
    }

    if ( $statement->columnCount() < 2 ) {
      throw new QueryRequirementException( "FetchPairs requires two columns to be selected" );
    }

    $results = [];

    while ( $row = $statement->fetch( \PDO::FETCH_NUM ) ) {

      // IMPORTANT: no matter how many columns are returned, result set only uses two
      $results[ $row[0] ] = $row[1];

    } // while fetch

    return $results;

  } // extractPairValues


  /**
   * {@inheritDoc}
   *
   * @return \PDOStatement
   */
  protected function _execute( $table, $sql, array $parameters = null, $use_master = false, $retries = 0 ) {

    // Results are saved to these variables, which are pushed out through post execute event
    $exception    = null;
    $statement    = null;
    $using_master = false;

    $post_emit = ( function( $statement, $parameters, $using_master, $exception = null ) {
      $this->_dispatcher->dispatch( self::EVENT_QUERY_POST_EXECUTE, new QueryEvent( $statement, $parameters, $using_master, $exception ) );
    } );

    // Fire pre-execute event
    // NOTE: this pre-execution hook only knows about master \/intention\/, not connection management rules
    $this->_dispatcher->dispatch( self::EVENT_QUERY_PRE_EXECUTE, new QueryEvent( $sql, $parameters, $use_master ) );

    try {

      $db = ( $use_master )
            ? $this->_getMasterAdapter( $table )
            : $this->_getReplicaAdapter( $table );

      // NOTE: the existing flag is the *intention* to use master, not the determination by the underlying connection manager
      // >>>>  expose actual usage in the events
      $using_master = $this->_connection->isUsingMaster( $table );

       // IMPORTANT: save result to variable to allow it to be picked up in finally block
      $statement = $db->prepare( $sql );

      // NOTE: result isn't captured, error level is set to exception
      $statement->execute( $parameters );

      $post_emit( $statement, $parameters, $using_master );

      return $statement; // Statement is container returned, results can be fetched from it

    } // try

    catch( \PDOException $e ) {

      $message   = sprintf( 'Query Exception: %s', $e->getMessage() );
      $exception = new Exceptions\QueryException( $message, null, $e );

      $statement = $statement ?: $sql; // Swap entry if there isn't a statement to provide

      // Ensure post-execute event is still fired
      $post_emit( $statement, $parameters, $using_master, $exception );

      // IMPORTANT: do not attempt to re-execute command is already in transaction
      if ( $this->isInTransaction() ) {
        throw $exception;
      }

      $recursion = ( $retries !== 0 ); // Only a single recursion is allowed

      // Unfortunately, the only way to detect this specific issue (server gone away) is a string match
      if ( !$recursion && stripos( $e->getMessage(), self::MESSAGE_SERVER_GONE_AWAY ) !== false ) {

        $db = null; // Hopefully excluded from ref-count in PDO

        $this->_reconnectAdapter();

        ++$retries;

        // IMPORTANT: re-attempt statement execution, using retry to prevent infinite recursion
        return $this->_execute( $table, $sql, $parameters, $use_master, $retries );

      } // if message = gone away

      throw $exception;

    } // catch PDOException

  } // _execute


  /**
   * @param string|null $table if known, will be used to further segment connection handling
   * @param string $sql        query to prepare and execute
   * @param array  $parameters data to be used in $sql
   *
   * @return PDOStatement
   */
  protected function _executeMaster( $table, $sql, array $parameters ) {

    return $this->_execute( $table, $sql, $parameters, true );

  } // _executeMaster


  /**
   * Reattempts a connection from an adapter that has gone stale
   *
   * @param \PDO $adapter
   */
  protected function _reconnectAdapter() {

    return $this->_connection->reconnect();

  } // _reconnectAdapter

} // PdoAdapter
