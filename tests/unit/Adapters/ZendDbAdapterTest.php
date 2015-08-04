<?php

namespace Behance\NBD\Dbal\Adapters;

use Behance\NBD\Dbal\Events\QueryEvent;
use Behance\NBD\Dbal\Test\BaseTest;

use Symfony\Component\EventDispatcher\EventDispatcher;
use Zend\Db\ResultSet\ResultSet as ZendResultSet;
use Zend\Db\Adapter\Exception\InvalidQueryException as ZendInvalidQueryException;
use Zend\Db\Adapter\Exception\RuntimeException as ZendRuntimeException;

class ZendDbAdapterTest extends BaseTest {

  private $_target             = 'Behance\NBD\Dbal\Adapters\ZendDbAdapter',
          $_connection_service = 'Behance\NBD\Dbal\Services\ConnectionService',
          $_db                 = 'Zend\Db\Adapter\Adapter',
          $_statement          = 'Zend\Db\Adapter\Driver\Pdo\Statement',
          $_driver             = 'Zend\Db\Adapter\Driver\Pdo\Pdo',
          $_result             = 'Zend\Db\Adapter\Driver\Pdo\Result',
          $_driver_connection  = 'Zend\Db\Adapter\Driver\Pdo\Connection';

  private $_insert_data = [ 'abc' => 123, 'def' => 456 ],
          $_table       = 'my_table';


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function query( $master ) {

    $sql        = "SELECT * FROM abc WHERE def = ? && ghi = ?";
    $params     = [ 123, 456 ];
    $connect_fx = ( $master )
                  ? 'getMaster'
                  : 'getReplica';

    $connection = $this->_getDisabledMock( $this->_connection_service, [ $connect_fx ] );
    $db         = $this->_getDisabledMock( $this->_db, [ 'createStatement' ] );
    $statement  = $this->getMock( $this->_statement, [ 'execute' ] );
    $result     = $this->getMock( $this->_result, [ 'isQueryResult' ] );

    $adapter    = new ZendDbAdapter( $connection );

    $connection->expects( $this->once() )
      ->method( $connect_fx )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'createStatement' )
      ->with( $sql, $params )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'execute' )
      ->will( $this->returnValue( $result ) );

    $result->expects( $this->once() )
      ->method( 'isQueryResult' )
      ->will( $this->returnValue( false ) );

    if ( $master ) {
      $this->assertSame( $result, $adapter->queryMaster( $sql, $params ) );
    }
    else {
      $this->assertSame( $result, $adapter->query( $sql, $params ) );
    }

  } // query


  /**
   * @test
   * @dataProvider boolProvider
   * @expectedException \Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function queryBadStatement( $master ) {

    $sql        = "INVALID--[SELECT * FROM abc WHERE def = ? && ghi = ?]--INVALID";
    $params     = [ 123, 456 ];
    $connect_fx = ( $master )
                  ? 'getMaster'
                  : 'getReplica';

    $connection = $this->_getDisabledMock( $this->_connection_service, [ $connect_fx ] );
    $db         = $this->_getDisabledMock( $this->_db, [ 'createStatement' ] );

    $exception  = new ZendInvalidQueryException( "Statement could not be executed" );
    $dispatcher = new EventDispatcher();
    $adapter    = new ZendDbAdapter( $connection, $dispatcher );

    $statement  = $this->getMock( $this->_statement, [ 'execute' ] );

    $connection->expects( $this->once() )
      ->method( $connect_fx )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'createStatement' )
      ->with( $sql, $params )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'execute' )
      ->will( $this->throwException( $exception ) );

    $dispatcher->addListener( ZendDbAdapter::EVENT_QUERY_POST_EXECUTE, function( QueryEvent $event ) use ( $statement, $exception ) {

      $this->assertSame( $statement, $event->getStatement() );
      $this->assertNull( $event->getResult() );
      $this->assertTrue( $event->hasException() );
      $this->assertInstanceOf( 'Behance\NBD\Dbal\Exceptions\InvalidQueryException', $event->getException() );
      $this->assertFalse( $event->hasResult() );

    } );

    if ( $master ) {
      $adapter->queryMaster( $sql, $params );
    }
    else {
      $adapter->query( $sql, $params );
    }

  } // queryBadStatement


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function queryReconnect( $master ) {

    $sql        = "SELECT * FROM abc WHERE def = ? && ghi = ?";
    $params     = [ 123, 456 ];
    $connect_fx = ( $master )
                  ? 'getMaster'
                  : 'getReplica';

    $dispatcher = new EventDispatcher();
    $connection = $this->_getDisabledMock( $this->_connection_service, [ $connect_fx ] );
    $adapter    = new ZendDbAdapter( $connection, $dispatcher );
    $db         = $this->_getDisabledMock( $this->_db, [ 'createStatement', 'getDriver' ] );
    $driver         = $this->_getDisabledMock( $this->_driver, [ 'getConnection' ] );
    $driver_connect = $this->getMock( $this->_driver_connection, [ 'disconnect', 'connect' ] );

    $statement  = $this->getMock( $this->_statement, [ 'execute' ] );
    $result     = $this->getMock( $this->_result, [ 'isQueryResult' ] );

    $connection->expects( $this->once() )
      ->method( $connect_fx )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'createStatement' )
      ->with( $sql, $params )
      ->will( $this->returnValue( $statement ) );

    $db->expects( $this->once() )
      ->method( 'getDriver' )
      ->will( $this->returnValue( $driver ) );

    $driver->expects( $this->once() )
      ->method( 'getConnection' )
      ->will( $this->returnValue( $driver_connect ) );

    $driver_connect->expects( $this->once() )
      ->method( 'disconnect' );

    $driver_connect->expects( $this->once() )
      ->method( 'connect' );

    $exception  = new \Zend\Db\Adapter\Exception\RuntimeException( "Mysql " . ZendDbAdapter::MESSAGE_SERVER_GONE_AWAY );
    $execute_ix = 0;

    $statement->expects( $this->exactly( 2 ) )
      ->method( 'execute' )
      ->will( $this->returnCallback( function() use ( &$execute_ix, $result, $exception ) {

        if ( !$execute_ix ) {
          ++$execute_ix;
          throw $exception;
        }

        return $result;

      } ) );

    $result->expects( $this->once() )
      ->method( 'isQueryResult' )
      ->will( $this->returnValue( false ) );

    $event_count = 0;
    $dispatcher->addListener( ZendDbAdapter::EVENT_QUERY_POST_EXECUTE, function( QueryEvent $event ) use ( &$event_count, $statement, $exception, $result ) {

      $this->assertSame( $statement, $event->getStatement() );

      if ( $event_count === 0 ) {

        $this->assertNull( $event->getResult() );
        $this->assertTrue( $event->hasException() );
        $this->assertInstanceOf( 'Behance\NBD\Dbal\Exceptions\Exception', $event->getException() );
        $this->assertFalse( $event->hasResult() );

      } // if event_count = 0

      else {

        $this->assertSame( $result, $event->getResult() );
        $this->assertFalse( $event->hasException() );
        $this->assertNull( $event->getException() );
        $this->assertTrue( $event->hasResult() );

      } // else (event_count != 0)

      ++$event_count;

    } );

    if ( $master ) {
      $this->assertSame( $result, $adapter->queryMaster( $sql, $params ) );
    }
    else {
      $this->assertSame( $result, $adapter->query( $sql, $params ) );
    }

    $this->assertEquals( 2, $event_count );

  } // queryReconnect


  /**
   * @test
   * @dataProvider boolProvider
   * @expectedException Behance\NBD\Dbal\Exceptions\QueryException
   */
  public function queryReconnectBad( $master ) {

    $sql        = "SELECT * FROM abc WHERE def = ? && ghi = ?";
    $params     = [ 123, 456 ];
    $connect_fx = ( $master )
                  ? 'getMaster'
                  : 'getReplica';

    $connection = $this->_getDisabledMock( $this->_connection_service, [ $connect_fx ] );
    $adapter    = $this->getMock( $this->_target, null, [ $connection ] );
    $db         = $this->_getDisabledMock( $this->_db, [ 'createStatement', 'getDriver' ] );
    $driver         = $this->_getDisabledMock( $this->_driver, [ 'getConnection' ] );
    $driver_connect = $this->getMock( $this->_driver_connection, [ 'disconnect', 'connect' ] );

    $statement  = $this->getMock( $this->_statement, [ 'execute' ] );

    $connection->expects( $this->once() )
      ->method( $connect_fx )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'createStatement' )
      ->with( $sql, $params )
      ->will( $this->returnValue( $statement ) );

    $db->expects( $this->once() )
      ->method( 'getDriver' )
      ->will( $this->returnValue( $driver ) );

    $driver->expects( $this->once() )
      ->method( 'getConnection' )
      ->will( $this->returnValue( $driver_connect ) );

    $driver_connect->expects( $this->once() )
      ->method( 'disconnect' );

    $driver_connect->expects( $this->once() )
      ->method( 'connect' );

    $statement->expects( $this->exactly( 2 ) )
      ->method( 'execute' )
      ->will( $this->throwException( new ZendRuntimeException( "Mysql " . ZendDbAdapter::MESSAGE_SERVER_GONE_AWAY ) ) );

    if ( $master ) {
      $adapter->queryMaster( $sql, $params );
    }
    else {
      $adapter->query( $sql, $params );
    }

  } // queryReconnectBad


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function queryResultSet( $master ) {

    $sql        = "SELECT * FROM abc WHERE def = ? && ghi = ?";
    $params     = [ 123, 456 ];
    $connect_fx = ( $master )
                  ? 'getMaster'
                  : 'getReplica';

    $pdo        = $this->getMock( '\PDOStatement' );
    $connection = $this->_getDisabledMock( $this->_connection_service, [ $connect_fx ] );
    $db         = $this->_getDisabledMock( $this->_db, [ 'createStatement' ] );
    $statement  = $this->getMock( $this->_statement, [ 'execute' ] );
    $result     = $this->getMock( $this->_result, [ 'isQueryResult' ] );
    $result->initialize( $pdo, [ 'abc', 'xyz' ] );

    $adapter    = new ZendDbAdapter( $connection );

    $connection->expects( $this->once() )
      ->method( $connect_fx )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'createStatement' )
      ->with( $sql, $params )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'execute' )
      ->will( $this->returnValue( $result ) );

    $result->expects( $this->once() )
      ->method( 'isQueryResult' )
      ->will( $this->returnValue( true ) );

    $resultset = $this->_getDisabledMock( 'Zend\Db\ResultSet\ResultSet', [ 'initialize' ] );
    $resultset->expects( $this->once() )
      ->method( 'initialize' )
      ->with( $result )
      ->will( $this->returnValue( $resultset ) );

    if ( $master ) {
      $this->assertSame( $resultset, $adapter->queryMaster( $sql, $params, $resultset ) );
    }
    else {
      $this->assertSame( $resultset, $adapter->query( $sql, $params, $resultset ) );
    }

  } // queryResultSet


  /**
   * @return array
   */
  public function boolProvider() {

    return [
        [ true ],
        [ false ]
    ];

  } // boolProvider


  /**
   * @test
   */
  public function insert() {

    $connection = $this->_getDisabledMock( $this->_connection_service, [ 'getMaster' ] );
    $adapter    = $this->getMock( $this->_target, [ '_getSqlBuilder' ], [ $connection ] );
    $db         = $this->_getDisabledMock( $this->_db );
    $sql        = $this->_getDisabledMock( 'Zend\Db\Sql\Sql', [ 'insert', 'prepareStatementForSqlObject' ] );
    $insert     = $this->getMock( 'Zend\Db\Sql\Insert' );
    $statement  = $this->getMock( $this->_statement, [ 'execute' ] );
    $result     = $this->getMock( $this->_result );

    $connection->expects( $this->once() )
      ->method( 'getMaster' )
      ->will( $this->returnValue( $db ) );

    $adapter->expects( $this->once() )
      ->method( '_getSqlBuilder' )
      ->with( $db, $this->_table )
      ->will( $this->returnValue( $sql ) );

    $sql->expects( $this->once() )
      ->method( 'insert' )
      ->will( $this->returnValue( $insert ) );

    $sql->expects( $this->once() )
      ->method( 'prepareStatementForSqlObject' )
      ->with( $insert )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'execute' )
      ->will( $this->returnValue( $result ) );

    $adapter->insert( $this->_table, $this->_insert_data );

  } // insert


  /**
   * @test
   */
  public function insertIgnore() {

    $query_result = 12345;

    $adapter    = $this->_getDisabledMock( $this->_target, [ '_getMasterAdapter', '_getSqlBuilder', '_getInsertIgnoreSqlBuilder', '_execute' ] );
    $db         = $this->_getDisabledMock( $this->_db );
    $statement  = $this->getMock( $this->_statement );
    $sql        = $this->_getDisabledMock( 'Zend\Db\Sql\Sql', [ 'prepareStatementForSqlObject' ] );
    $insert     = $this->getMock( 'Behance\NBD\Dbal\Adapters\Sql\InsertIgnore', [ 'values' ] );
    $result     = $this->getMock( $this->_result, [ 'getGeneratedValue' ] );

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $db ) );

    $adapter->expects( $this->once() )
      ->method( '_getSqlBuilder' )
      ->with( $db, $this->_table )
      ->will( $this->returnValue( $sql ) );

    $adapter->expects( $this->once() )
      ->method( '_getInsertIgnoreSqlBuilder' )
      ->will( $this->returnValue( $insert ) );

    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( $db, $statement )
      ->will( $this->returnValue( $result ) );

    $insert->expects( $this->once() )
      ->method( 'values' )
      ->with( $this->_insert_data );

    $sql->expects( $this->once() )
      ->method( 'prepareStatementForSqlObject' )
      ->with( $insert )
      ->will( $this->returnValue( $statement ) );

    $result->expects( $this->once() )
      ->method( 'getGeneratedValue' )
      ->will( $this->returnValue( $query_result ) );

    $this->assertEquals( $query_result, $adapter->insertIgnore( $this->_table, $this->_insert_data ) );

  } // insertIgnore


  /**
   * Ensures that unmocked version of InsertIgnore object is processed correctly
   *
   * @test
   */
  public function insertIgnoreRaw() {

    $params       = [ 'abc' => 123, 'xyz' => 456 ];
    $table        = 'abcdef';
    $query_result = 12345;

    $adapter    = $this->_getDisabledMock( $this->_target, [ '_getMasterAdapter', '_getSqlBuilder', '_execute' ] );
    $db         = $this->_getDisabledMock( $this->_db );
    $statement  = $this->getMock( $this->_statement );
    $sql        = $this->_getDisabledMock( 'Zend\Db\Sql\Sql', [ 'prepareStatementForSqlObject' ] );
    $result     = $this->getMock( $this->_result, [ 'getGeneratedValue' ] );

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $db ) );

    $adapter->expects( $this->once() )
      ->method( '_getSqlBuilder' )
      ->will( $this->returnValue( $sql ) );

    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->will( $this->returnValue( $result ) );

    $sql->expects( $this->once() )
      ->method( 'prepareStatementForSqlObject' )
      ->will( $this->returnValue( $statement ) );

    $result->expects( $this->once() )
      ->method( 'getGeneratedValue' )
      ->will( $this->returnValue( $query_result ) );

    $this->assertEquals( $query_result, $adapter->insertIgnore( $table, $params ) );

  } // insertIgnoreRaw


  /**
   * @test
   */
  public function quote() {

    $value      = 'won\'t matter';
    $result     = "won\\'t matter";

    $db         = $this->_getDisabledMock( $this->_db, [ 'quoteValue' ] );
    $connection = $this->_getDisabledMock( $this->_connection_service, [ 'getMaster' ] );
    $adapter    = $this->getMock( $this->_target, [ '_getReplicaAdapter' ], [ $connection ] );

    $adapter->expects( $this->once() )
      ->method( '_getReplicaAdapter' )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'quoteValue' )
      ->with( $value )
      ->will( $this->returnValue( $result ) );

    $this->assertSame( $result, $adapter->quote( $value ) );

  } // quote


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function updateNoData() {

    $connection = $this->_getDisabledMock( $this->_connection_service );
    $adapter    = new ZendDbAdapter( $connection );
    $adapter->update( 'abc', [], '' );

  } // updateNoData


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\QueryRequirementException
   */
  public function updateNoWhere() {

    $connection = $this->_getDisabledMock( $this->_connection_service );
    $adapter    = new ZendDbAdapter( $connection );
    $adapter->update( 'abc', [ 'xyz' => 123 ], '' );

  } // updateNoWhere


  /**
   * @test
   * @dataProvider badWhereProvider
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function updateBadWhere( $where ) {

    $connection = $this->_getDisabledMock( $this->_connection_service );
    $adapter    = new ZendDbAdapter( $connection );
    $adapter->update( 'abc', [ 'xyz' => 123 ], $where );

  } // updateBadWhere


  /**
   * @return array
   */
  public function badWhereProvider() {

    return [
        [ 456 ],
        [ new \stdClass() ],
    ];

  } // badWhereProvider

  /**
   * @test
   * @dataProvider whereProvider
   */
  public function update( $where ) {

    $table      = 'abcdef';
    $params     = [ 'abc' => 123, 'xyz' => 456 ];

    $connection = $this->_getDisabledMock( $this->_connection_service, [ 'getMaster' ] );
    $adapter    = $this->getMock( $this->_target, [ '_getSqlBuilder' ], [ $connection ] );
    $db         = $this->_getDisabledMock( $this->_db );
    $sql        = $this->_getDisabledMock( 'Zend\Db\Sql\Sql', [ 'update', 'prepareStatementForSqlObject' ] );
    $update     = $this->getMock( 'Zend\Db\Sql\Update' );
    $statement  = $this->getMock( $this->_statement, [ 'execute' ] );
    $result     = $this->getMock( $this->_result, [ 'getAffectedRows' ] );
    $affected   = 1;

    $connection->expects( $this->once() )
      ->method( 'getMaster' )
      ->will( $this->returnValue( $db ) );

    $adapter->expects( $this->once() )
      ->method( '_getSqlBuilder' )
      ->with( $db, $table )
      ->will( $this->returnValue( $sql ) );

    $sql->expects( $this->once() )
      ->method( 'update' )
      ->will( $this->returnValue( $update ) );

    $sql->expects( $this->once() )
      ->method( 'prepareStatementForSqlObject' )
      ->with( $update )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'execute' )
      ->will( $this->returnValue( $result ) );

    $result->expects( $this->once() )
      ->method( 'getAffectedRows' )
      ->will( $this->returnValue( $affected ) );

    $this->assertSame( $affected, $adapter->update( $table, $params, $where ) );

  } // update


  /**
   * @test
   * @dataProvider whereProvider
   */
  public function delete( $where ) {

    $table      = 'abcdef';

    $connection = $this->_getDisabledMock( $this->_connection_service, [ 'getMaster' ] );
    $adapter    = $this->getMock( $this->_target, [ '_getSqlBuilder' ], [ $connection ] );
    $db         = $this->_getDisabledMock( $this->_db );
    $sql        = $this->_getDisabledMock( 'Zend\Db\Sql\Sql', [ 'delete', 'prepareStatementForSqlObject' ] );
    $delete     = $this->getMock( 'Zend\Db\Sql\Delete' );
    $statement  = $this->getMock( $this->_statement, [ 'execute' ] );
    $result     = $this->getMock( $this->_result, [ 'getAffectedRows' ] );
    $affected   = 1;

    $connection->expects( $this->once() )
      ->method( 'getMaster' )
      ->will( $this->returnValue( $db ) );

    $adapter->expects( $this->once() )
      ->method( '_getSqlBuilder' )
      ->with( $db, $table )
      ->will( $this->returnValue( $sql ) );

    $sql->expects( $this->once() )
      ->method( 'delete' )
      ->will( $this->returnValue( $delete ) );

    $sql->expects( $this->once() )
      ->method( 'prepareStatementForSqlObject' )
      ->with( $delete )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'execute' )
      ->will( $this->returnValue( $result ) );

    $result->expects( $this->once() )
      ->method( 'getAffectedRows' )
      ->will( $this->returnValue( $affected ) );

    $this->assertSame( $affected, $adapter->delete( $table, $where ) );

  } // delete


  /**
   * @test
   * @dataProvider whereProvider
   */
  public function deleteRaw( $where ) {

    $table      = 'abcdef';

    $driver_connect = $this->_getDisabledMock( $this->_driver_connection );
    $driver         = $this->getMock( 'Zend\Db\Adapter\Driver\Pdo\Pdo', [ 'createStatement' ], [ $driver_connect ] );
    $connection     = $this->_getDisabledMock( $this->_connection_service, [ 'getMaster' ] );
    $db             = $this->getMock( $this->_db, [ 'getPlatform' ], [ $driver ] );
    $adapter        = $this->getMock( $this->_target, [ '_execute' ], [ $connection ] );
    $result         = $this->getMock( $this->_result, [ 'getAffectedRows' ] );
    $platform       = $this->getMock( 'Zend\Db\Adapter\Platform\Mysql', null );
    $affected       = 1;
    $statement      = $this->getMock( $this->_statement );

    $driver->expects( $this->atLeastOnce() )
      ->method( 'createStatement' )
      ->will( $this->returnValue( $statement ) );

    $db->expects( $this->atLeastOnce() )
      ->method( 'getPlatform' )
      ->will( $this->returnValue( $platform ) );

    $connection->expects( $this->once() )
      ->method( 'getMaster' )
      ->will( $this->returnValue( $db ) );

    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( $db, $this->anything() )
      ->will( $this->returnValue( $result ) );

    $result->expects( $this->once() )
      ->method( 'getAffectedRows' )
      ->will( $this->returnValue( $affected ) );

    $this->assertSame( $affected, $adapter->delete( $table, $where ) );

  } // deleteRaw


  /**
   * @return array
   */
  public function whereProvider() {

    return [
        "String"          => [ 'id=1' ],
        "Array Strings"   => [ [ 'id=1', 'second_id=2' ] ],
        "Array Key:Value" => [ [ 'id' => 1 ] ],
        "Array Key:Value" => [ [ 'id' => 1, 'second_id' => 2 ] ]

    ];

  } // whereProvider


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\QueryRequirementException
   */
  public function deleteNoWhere() {

    $connection = $this->_getDisabledMock( $this->_connection_service );
    $adapter    = new ZendDbAdapter( $connection );
    $adapter->delete( 'abc', '' );

  } // deleteNoWhere


  /**
   * @test
   * @dataProvider badWhereProvider
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function deleteBadWhere( $where ) {

    $connection = $this->_getDisabledMock( $this->_connection_service );
    $adapter    = new ZendDbAdapter( $connection );
    $adapter->delete( 'abc', $where );

  } // deleteBadWhere


  /**
   * @test
   */
  public function beginTransaction() {

    $connection     = $this->_getDisabledMock( $this->_connection_service, [ 'getMaster' ] );
    $adapter        = new ZendDbAdapter( $connection );
    $db             = $this->_getDisabledMock( $this->_db, [ 'getDriver' ] );
    $driver         = $this->_getDisabledMock( $this->_driver, [ 'getConnection' ] );
    $driver_connect = $this->getMock( $this->_driver_connection, [ 'beginTransaction' ] );
    $result         = true;

    $connection->expects( $this->once() )
      ->method( 'getMaster' )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'getDriver' )
      ->will( $this->returnValue( $driver ) );

    $driver->expects( $this->once() )
      ->method( 'getConnection' )
      ->will( $this->returnValue( $driver_connect ) );

    $driver_connect->expects( $this->once() )
      ->method( 'beginTransaction' )
      ->will( $this->returnValue( $result ) );

    $this->assertSame( $result, $adapter->beginTransaction() );

  } // beginTransaction


  /**
   * @test
   */
  public function commit() {

    $connection     = $this->_getDisabledMock( $this->_connection_service, [ 'getMaster' ] );
    $adapter        = new ZendDbAdapter( $connection );
    $db             = $this->_getDisabledMock( $this->_db, [ 'getDriver' ] );
    $driver         = $this->_getDisabledMock( $this->_driver, [ 'getConnection' ] );
    $driver_connect = $this->getMock( $this->_driver_connection, [ 'commit' ] );
    $result         = true;

    $connection->expects( $this->once() )
      ->method( 'getMaster' )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'getDriver' )
      ->will( $this->returnValue( $driver ) );

    $driver->expects( $this->once() )
      ->method( 'getConnection' )
      ->will( $this->returnValue( $driver_connect ) );

    $driver_connect->expects( $this->once() )
      ->method( 'commit' )
      ->will( $this->returnValue( $result ) );

    $this->assertSame( $result, $adapter->commit() );

  } // commit


  /**
   * @test
   */
  public function rollback() {

    $connection     = $this->_getDisabledMock( $this->_connection_service, [ 'getMaster' ] );
    $adapter        = new ZendDbAdapter( $connection );
    $db             = $this->_getDisabledMock( $this->_db, [ 'getDriver' ] );
    $driver         = $this->_getDisabledMock( $this->_driver, [ 'getConnection' ] );
    $driver_connect = $this->getMock( $this->_driver_connection, [ 'rollback' ] );
    $result         = true;

    $connection->expects( $this->once() )
      ->method( 'getMaster' )
      ->will( $this->returnValue( $db ) );

    $db->expects( $this->once() )
      ->method( 'getDriver' )
      ->will( $this->returnValue( $driver ) );

    $driver->expects( $this->once() )
      ->method( 'getConnection' )
      ->will( $this->returnValue( $driver_connect ) );

    $driver_connect->expects( $this->once() )
      ->method( 'rollback' )
      ->will( $this->returnValue( $result ) );

    $this->assertSame( $result, $adapter->rollback() );

  } // rollback

} // ZendDbAdapterTest
