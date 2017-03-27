<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\AdapterInterface;
use Behance\NBD\Dbal\ConnectionService;

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
  abstract public function fetchOne( $sql, array $parameters = null, $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function fetchRow( $sql, array $parameters = null, $master = false );



  /**
   * {@inheritDoc}
   */
  abstract public function fetchColumn( $sql, array $parameters = null, $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function fetchAll( $sql, array $parameters = null, $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function fetchAssoc( $sql, array $parameters = null, $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function fetchPairs( $sql, array $parameters = null, $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function getOne( $table, $column, $where, $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function getRow( $table, $where = '', $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function getColumn( $table, $column, $where, $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function getAll( $table, $where, $master = false );


  /**
   * {@inheritDoc}
   */
  abstract public function getAssoc( $table, $where, $master = false );


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
   * TODO: this is still using a PDO-specific typehint
   */
  abstract public function quote( $value, $type = \PDO::PARAM_STR );


  /**
   * {@inheritDoc}
   */
  public function bindEvent( $event_name, callable $handler ) {

    $this->_dispatcher->addListener( $event_name, $handler );

  } // bindEvent


  /**
   * @alias of ->fetchColumn()
   */
  public function fetchCol( $sql, array $parameters = null, $master = false ) {

    return $this->fetchColumn( $sql, $parameters, $master );

  } // fetchCol


  /**
   * {@inheritDoc}
   */
  public function getCol( $table, $column, $where, $master = false ) {

    return $this->getColumn( $table, $column, $where, $master );

  } // getCol


  /**
   * {@inheritDoc}
   */
  public function query( $sql, array $parameters = null, $use_master = false ) {

    return $this->_execute( null, $sql, $parameters, $use_master );

  } // query


  /**
   * {@inheritDoc}
   */
  public function queryMaster( $sql, array $parameters = null ) {

    return $this->_execute( null, $sql, $parameters, true );

  } // queryMaster


  /**
   * {@inheritDoc}
   */
  public function queryTable( $table, $sql, array $parameters = null, $use_master = false ) {

    return $this->_execute( $table, $sql, $parameters, $use_master );

  } // queryTable


  /**
   * {@inheritDoc}
   */
  public function queryTableMaster( $table, $sql, array $parameters = null ) {

    return $this->_execute( $table, $sql, $parameters, true );

  } // queryTableMaster


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
   * NOTE: not exposed publicly to prevent accidental intervention of the retries count
   *
   * @param string|null $table if known, will be used to further segment connection handling
   * @param string $sql        query to prepare and execute
   * @param array  $parameters data to be used in $sql
   * @param bool   $use_master whether to use write or read connection for statement
   * @param int    $retries    number of attempts executed on $statement, used to prevent infinite recursion, one retry is max
   *
   * @return mixed
   */
  abstract protected function _execute( $table, $sql, array $parameters = null, $use_master = false, $retries = 0 );


  /**
   * @param string|null $table if known, will be used to further segment connection handling
   *
   * @return PDO
   */
  protected function _getMasterAdapter( $table = null ) {

    return $this->_connection->getMaster( $table );

  } // _getMasterAdapter


  /**
   * @param string|null $table if known, will be used to further segment connection handling
   *
   * @return PDO
   */
  protected function _getReplicaAdapter( $table = null ) {

    return $this->_connection->getReplica( $table );

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
   * Quotes columns to protect against keyword issues
   *
   * @param array $columns
   *
   * @return array
   */
  protected function _quoteColumns( array $columns ) {

    return array_map( function( $column ) {
      return $this->_quoteColumn( $column );
    }, $columns );

  } // _quoteColumns

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
