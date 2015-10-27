<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\AdapterInterface;
use Behance\NBD\Dbal\ConnectionService;
use Behance\NBD\Dbal\Exceptions\QueryRequirementException;

use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Provides high-level implementation common to all DbAdapters
 */
abstract class AdapterAbstract implements AdapterInterface {

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
   * {@inheritDoc}
   */
  abstract public function insert( $table, array $data );


  /**
   * {@inheritDoc}
   */
  abstract public function insertIgnore( $table, array $data );


  /**
   * {@inheritDoc}
   */
  abstract public function insertOnDuplicateUpdate( $table, array $data, array $update_data );


  /**
   * {@inheritDoc}
   */
  abstract public function update( $table, array $data, $where );


  /**
   * {@inheritDoc}
   */
  abstract public function delete( $table, $where );


  /**
   * {@inheritDoc}
   */
  abstract public function beginTransaction();


  /**
   * {@inheritDoc}
   */
  abstract public function commit();


  /**
   * {@inheritDoc}
   */
  abstract public function rollBack();


  /**
   * {@inheritDoc}
   */
  abstract public function query( $sql, array $parameters = null, $use_master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function queryMaster( $sql, array $parameters = null );


  /**
   * {@inheritDoc}
   */
  abstract public function quote( $value, $type = \PDO::PARAM_STR );


  /**
   * {@inheritDoc}
   */
  public function bindEvent( $event_name, callable $handler ) {

    $this->_dispatcher->addListener( $event_name, $handler );

  } // bindEvent


  /**
   * {@inheritDoc}
   */
  public function fetchOne( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    if ( $statement->columnCount() === 0 ) {
      return;
    }

    // @see http://php.net/manual/en/pdostatement.fetchcolumn.php
    // Cannot use fetchcolumn if boolean values will fail row check
    $fetched = $statement->fetch( \PDO::FETCH_NUM );

    // IMPORTANT: no matter the result size, the return type is only ever the first column
    return ( empty( $fetched[0] ) )
           ? null
           : $fetched[0];

  } // fetchOne


  /**
   * {@inheritDoc}
   */
  public function fetchRow( $sql, array $parameters = null, $master = false ) {

    $result = $this->query( $sql, $parameters, $master );

    // This is 0 on an empty result set
    if ( $result->columnCount() === 0 ) {
      return [];
    }

    $row = $result->fetch( \PDO::FETCH_ASSOC );

    // IMPORTANT: no matter the result size, the return type is the first row
    return ( empty( $row ) )
           ? []
           : $row;

  } // fetchRow


  /**
   * @alias of ->fetchColumn()
   */
  public function fetchCol( $sql, array $parameters = null, $master = false ) {

    return $this->fetchColumn( $sql, $parameters, $master );

  } // fetchCol


  /**
   * {@inheritDoc}
   */
  public function fetchColumn( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    return ( $statement->columnCount() === 0 )
           ? []
           : $statement->fetchAll( \PDO::FETCH_COLUMN );

  } // fetchColumn


  /**
   * {@inheritDoc}
   */
  public function fetchAll( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

    return ( $statement->columnCount() === 0 )
           ? []
           : $statement->fetchAll( \PDO::FETCH_ASSOC );

  } // fetchAll


  /**
   * {@inheritDoc}
   */
  public function fetchAssoc( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );
    $results   = [];

    while ( $row = $statement->fetch( \PDO::FETCH_ASSOC ) ) {

      // Retrieves the first associative value from the array
      $values = array_values( array_slice( $row, 0, 1 ) );

      $results[ $values[ 0 ] ] = $row;

    } // while fetch

    return $results;

  } // fetchAssoc


  /**
   * {@inheritDoc}
   */
  public function fetchPairs( $sql, array $parameters = null, $master = false ) {

    $statement = $this->query( $sql, $parameters, $master );

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

  } // fetchPairs


  /**
   * {@inheritDoc}
   */
  public function getOne( $table, $column, $where, $master = false ) {

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );
    $query = sprintf( "SELECT %s FROM %s %s", $this->_quoteColumn( $column ), $this->_quoteTable( $table ), $where_sql );

    $parameters = ( empty( $where_prepared ) )
                  ? null
                  : $where_prepared;

    return $this->fetchOne( $query, $parameters, $master );

  } // getOne


  /**
   * {@inheritDoc}
   */
  public function getRow( $table, $where = '', $master = false ) {

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );
    $query = sprintf( "SELECT * FROM %s %s", $this->_quoteTable( $table ), $where_sql );

    $parameters = ( empty( $where_prepared ) )
                  ? null
                  : $where_prepared;

    return $this->fetchRow( $query, $parameters, $master );

  } // getRow


  /**
   * {@inheritDoc}
   */
  public function getCol( $table, $column, $where, $master = false ) {

    return $this->getColumn( $table, $column, $where, $master );

  } // getCol


  /**
   * {@inheritDoc}
   */
  public function getColumn( $table, $column, $where, $master = false ) {

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );

    $query = sprintf( "SELECT %s FROM %s %s", $this->_quoteColumn( $column ), $this->_quoteTable( $table ), $where_sql );
    $parameters = ( empty( $where_prepared ) )
                  ? null
                  : $where_prepared;

    return $this->fetchColumn( $query, $parameters, $master );

  } // getColumn


  /**
   * {@inheritDoc}
   */
  public function getAll( $table, $where, $master = false ) {

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );
    $query = sprintf( "SELECT * FROM %s %s", $this->_quoteTable( $table ), $where_sql );

    $parameters = ( empty( $where_prepared ) )
                  ? null
                  : $where_prepared;

    return $this->fetchAll( $query, $parameters, $master );

  } // getAll


  /**
   * {@inheritDoc}
   */
  public function getAssoc( $table, $where, $master = false ) {

    list( $where_sql, $where_prepared ) = $this->_buildWhere( $where );
    $query = sprintf( "SELECT * FROM %s %s", $this->_quoteTable( $table ), $where_sql );

    $parameters = ( empty( $where_prepared ) )
                  ? null
                  : $where_prepared;

    return $this->fetchAssoc( $query, $parameters, $master );

  } // getAssoc


  /**
   * {@inheritDoc}
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
   * @return PDO
   */
  protected function _getMasterAdapter() {

    return $this->_connection->getMaster();

  } // _getMasterAdapter


  /**
   * @return PDO
   */
  protected function _getReplicaAdapter() {

    return $this->_connection->getReplica();

  } // _getReplicaAdapter


  /**
   * @param mixed $where
   *
   * @return array  [ 0 => where SQL statement, 1 => additional prepared values ]
   */
  protected function _buildWhere( $where ) {

    $prepared = [];
    $noop     = [ '', $prepared ];

    if ( empty( $where ) ) {
      return $noop;
    }

    $sql = 'WHERE ';

    // Only objects, strings and arrays are supported WHERE types
    if ( !is_object( $where ) && !is_string( $where ) && !is_array( $where ) ) {
      throw new Exceptions\InvalidQueryException( "Invalid format for WHERE: " . var_export( $where, 1 ) );
    }

    if ( is_object( $where ) ) {

      $this->_checkSqlObject( $where );

      $sql .= (string)$where;
      return [ $sql, $prepared ];

    } // if is_object

    if ( is_string( $where ) ) {
      $sql .= $where;
      return [ $sql, $prepared ];
    }

    // Assume array going forward

    if ( !$this->_isAssociativeArray( $where ) ) {
      $sql .= implode( ' AND ', $where );
      return [ $sql, $prepared ];
    }

    $complex_where = [];

    foreach ( $where as $key => $value ) {

      $quoted_column = $this->_quoteColumn( $key );

      if ( is_object( $value ) ) {

        $this->_checkSqlObject( $value );

        $complex_where[] = $quoted_column . ' = ' . (string)$value; // Value will not be quoted

      } // if is_object

      elseif ( $value === null ) {
        $complex_where[] = $quoted_column . ' IS NULL';
      }

      else {
        $complex_where[] = $quoted_column . ' = ?';
        $prepared[]      = $value;
      }

    } // foreach where

    return [ $sql . implode( ' AND ', $complex_where ), $prepared ];

  } // _buildWhere


  /**
   * @param array $data   column => value parameters
   *
   * @return array [ 0 => reflowed input without unprepared parameters, 1 => ordered positional keys for columns ]
   */
  protected function _prepPositionalValues( array $data, $named = false ) {

    if ( !$this->_isAssociativeArray( $data ) ) {
      throw new Exceptions\InvalidQueryException( "Data must be an associative array" );
    }

    $set = [];

    foreach ( $data as $column => $value ) {

      if ( is_object( $value ) ) {

        $this->_checkSqlObject( $value ); // Throws exception on failure

        // This will not be a prepared argument
        $value = (string)$value;
        $set[] = ( $named )
                 ? sprintf( '%s = %s', $this->_quoteColumn( $column ), $value )
                 : $value;

        unset( $data[ $column ] );

      } // if is_object

      else {
        $set[] = ( $named )
                 ? sprintf( '%s = ?', $this->_quoteColumn( $column ), '?' )
                 : '?';
      }

    } // foreach data

    return [ $data, $set ];

  } // _prepPositionalValues


  /**
   * @param array $data
   *
   * @return array [ 0 => reflowed input without unprepared parameters, 1 => ordered positional keys for columns ]
   */
  protected function _prepPositionValuePairs( array $data ) {

    return $this->_prepPositionalValues( $data, true );

  } // _prepPositionValuePairs


  /**
   * @param string $column
   *
   * @return string
   */
  protected function _quoteTable( $table ) {

    return "`{$table}`";

  } // _quoteTable


  /**
   * @param string $column
   *
   * @return string
   */
  protected function _quoteColumn( $column ) {

    return "`{$column}`";

  } // _quoteColumn


  /**
   * @see https://gist.github.com/Thinkscape/1965669
   *
   * @param array $array
   *
   * @return bool
   */
  protected function _isAssociativeArray( array $array ) {

    return ( array_values( $array ) !== $array );

  } // _isAssociativeArray


  /**
   * @throws Behance\NBD\Dbal\Exceptions\InvalidQueryException
   * @param mixed
   */
  protected function _checkSqlObject( $object ) {

    if ( !$this->_isSqlObject( $object ) ) {
      throw new Exceptions\InvalidQueryException( "Object cannot be converted to string" );
    }

  } // _checkSqlObject


  /**
   * SQL object is currently defined as something that can be converted to a string
   *
   * @param mixed $object
   *
   * @return bool
   */
  protected function _isSqlObject( $object ) {

    return ( is_object( $object ) && method_exists( $object, '__toString' ) );

  } // _isSqlObject

} // AdapterAbstract
