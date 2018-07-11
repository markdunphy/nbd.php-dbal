<?php

namespace Behance\NBD\Dbal\Adapters;

use Behance\NBD\Dbal\ConfigService;
use Behance\NBD\Dbal\ConnectionService;
use Behance\NBD\Dbal\DbalException;
use Behance\NBD\Dbal\Events\QueryEvent;
use Behance\NBD\Dbal\Exceptions;
use Behance\NBD\Dbal\Sql;
use Behance\NBD\Dbal\Test\BaseTest;

use Pseudo\Pdo as PPDO;

use Symfony\Component\EventDispatcher\EventDispatcher;

class PdoAdapterTest extends BaseTest {

  private $_table       = 'my_table';
  private $_insert_data = [ 'abc' => 123, 'def' => 456 ];
  private $_update_data = [ 'ghi' => 789, 'created_on' => 0 ];

  private $_sample_resultset = [
      [
          'id'      => 1234,
          'enabled' => 1
      ],
      [
          'id'      => 5678,
          'enabled' => 0
      ],
  ];

  private $_sample_column = 'enabled';
  private $_sample_sql    = "SELECT `enabled` FROM `my_table` WHERE `abc` = ? AND `def` = ?";
  private $_sample_where  = [ 'abc' => 123, 'def' => 456 ];

  /**
   * @test
   * @dataProvider boolProvider
   */
  public function queryRaw( $master ) {

    $sql        = "SELECT * FROM abc WHERE def = ? && ghi = ?";
    $params     = [ 123, 456 ];
    $connect_fx = ( $master )
                  ? 'getMaster'
                  : 'getReplica';
    $connection = $this->_getDisabledMock( ConnectionService::class, [ $connect_fx ] );
    $adapter    = new PdoAdapter( $connection );
    $results    = $this->_sample_resultset;

    $db = new PPDO();
    $db->mock( $sql, $results, $params );
    $connection->expects( $this->once() )
      ->method( $connect_fx )
      ->will( $this->returnValue( $db ) );
    $statement = ( $master )
                 ? $adapter->queryMaster( $sql, $params )
                 : $adapter->query( $sql, $params );
    // NOTE: fetchAll is being run against raw PDOStatement object, not Adapter wrapper fetchAll method
    $this->assertSame( $results, $statement->fetchAll( \PDO::FETCH_ASSOC ) );

  } // queryRaw


  /**
   * @test
   * @dataProvider boolProvider
   * @expectedException \Behance\NBD\Dbal\Exceptions\QueryException
   */
  public function queryBadPrepare( $master ) {

    $sql    = "INVALID--[SELECT * FROM abc WHERE def = ? && ghi = ?]--INVALID";
    $params = [ 123, 456 ];

    $config = $this->_getDisabledMock( ConfigService::class, [ 'getMaster', 'getReplica' ] );
    $config->method( 'getMaster' )
      ->will( $this->returnValue( [] ) );
    $config->method( 'getReplica' )
      ->will( $this->returnValue( [] ) );

    $connection = $this->getMockBuilder( ConnectionService::class )
      ->setMethods( [ '_buildAdapter' ] )
      ->setConstructorArgs( [ $config ] )
      ->getMock();

    $pdo = $this->_getDisabledMock( \PDO::class, [ 'prepare' ] );

    $exception = new \PDOException( "Statement could not be prepared" );
    $adapter   = new PdoAdapter( $connection );

    $connection->expects( $this->atLeastOnce() )
      ->method( '_buildAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $pdo->expects( $this->once() )
      ->method( 'prepare' )
      ->will( $this->throwException( $exception ) );

    $adapter->bindEvent( PdoAdapter::EVENT_QUERY_POST_EXECUTE, function( QueryEvent $event ) use ( $sql, $exception, $params, $master ) {

      $this->assertSame( $master, $event->isUsingMaster() );
      $this->assertFalse( $event->hasStatement() );
      $this->assertTrue( $event->hasParameters() );
      $this->assertSame( $params, $event->getParameters() );
      $this->assertSame( $sql, $event->getQuery() );
      $this->assertTrue( $event->hasException() );
      $this->assertInstanceOf( Exceptions\QueryException::class, $event->getException() );
      $this->assertNotSame( $exception, $event->getException() );

    } ); // bindEvent

    $adapter->query( $sql, $params, $master );

  } // queryBadPrepare


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function queryReconnect( $master ) {

    $sql        = "SELECT * FROM abc WHERE def = ? && ghi = ?";
    $params     = [ 123, 456 ];
    $connect_fx = ( $master )
                  ? '_getMasterAdapter'
                  : '_getReplicaAdapter';

    $connection = $this->_getDisabledMock( ConnectionService::class, [ 'reconnect' ] );

    $connection->expects( $this->once() )
      ->method( 'reconnect' );

    $methods = [ '_getMasterAdapter', '_getReplicaAdapter' ];
    $adapter = $this->getMockBuilder( PdoAdapter::class )
      ->setMethods( $methods )
      ->setConstructorArgs( [ $connection ] )
      ->getMock();

    $pdo       = $this->_getDisabledMock( \PDO::class, [ 'prepare' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'execute' ] );

    $pdo->expects( $this->exactly( 2 ) )
      ->method( 'prepare' )
      ->will( $this->returnValue( $statement ) );

    $adapter->expects( $this->exactly( 2 ) )
      ->method( $connect_fx )
      ->will( $this->returnValue( $pdo ) );

    $exception = new \PDOException( "Mysql " . PdoAdapter::MESSAGE_SERVER_GONE_AWAY );

    $statement->expects( $this->exactly( 2 ) )
      ->method( 'execute' )
      ->will( $this->onConsecutiveCalls( $this->throwException( $exception ), true ) );

    $event_count = 0;
    $callback    = ( function( QueryEvent $event ) use ( &$event_count, $statement, $exception ) {

      $this->assertTrue( $event->hasStatement() );
      $this->assertSame( $statement, $event->getStatement() );

      if ( $event_count === 0 ) {

        $this->assertTrue( $event->hasException() );
        $this->assertInstanceOf( DbalException::class, $event->getException() );

      } // if event_count = 0

      else {

        $this->assertFalse( $event->hasException() );
        $this->assertNull( $event->getException() );

      } // else (event_count != 0)

      ++$event_count;

    } );

    $adapter->bindEvent( PdoAdapter::EVENT_QUERY_POST_EXECUTE, $callback );

    $result = ( $master )
              ? $adapter->queryMaster( $sql, $params )
              : $adapter->query( $sql, $params );

    $this->assertSame( $statement, $result );

    $this->assertEquals( 2, $event_count );

  } // queryReconnect


  /**
   * @test
   * @dataProvider badReconnectProvider
   * @expectedException Behance\NBD\Dbal\Exceptions\QueryException
   */
  public function queryReconnectBad( $master, $in_transaction ) {

    $sql        = "SELECT * FROM abc WHERE def = ? && ghi = ?";
    $params     = [ 123, 456 ];
    $connect_fx = ( $master )
                  ? '_getMasterAdapter'
                  : '_getReplicaAdapter';
    $methods    = [ 'isInTransaction', '_getMasterAdapter', '_getReplicaAdapter', '_reconnectAdapter' ];
    $connection = $this->_getDisabledMock( ConnectionService::class );
    $adapter    = $this->getMockBuilder( PdoAdapter::class )
      ->setMethods( $methods )
      ->setConstructorArgs( [ $connection ] )
      ->getMock();

    $adapter->method( 'isInTransaction' )
      ->willReturn( $in_transaction );

    $pdo       = $this->_getDisabledMock( \PDO::class, [ 'prepare' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'execute' ] );

    $expected = ( $in_transaction )
                ? 1
                : 2;

    $pdo->expects( $this->exactly( $expected ) )
      ->method( 'prepare' )
      ->will( $this->returnValue( $statement ) );

    $adapter->expects( $this->exactly( $expected ) )
      ->method( $connect_fx )
      ->will( $this->returnValue( $pdo ) );

    if ( $in_transaction ) {
      $adapter->expects( $this->never() )
        ->method( '_reconnectAdapter' );
    }
    else {
      $adapter->expects( $this->once() )
        ->method( '_reconnectAdapter' )
        ->will( $this->returnValue( $pdo ) );
    }

    $statement->expects( $this->atLeastOnce() )
      ->method( 'execute' )
      ->will( $this->throwException( new \PDOException( "Mysql " . PdoAdapter::MESSAGE_SERVER_GONE_AWAY ) ) );

    if ( $master ) {
      $adapter->queryMaster( $sql, $params );
    }
    else {
      $adapter->query( $sql, $params );
    }

  } // queryReconnectBad


  /**
   * @return array
   */
  public function badReconnectProvider() {

    return [
        [ false, false ],
        [ true, false ],
        [ false, true ],
        [ true, true ],
    ];

  } // badReconnectProvider


  /**
   * @test
   * @dataProvider insertDataProvider
   */
  public function insert( $insert_data ) {

    $insert_id = '12345';

    $adapter             = $this->_getDisabledMock( PdoAdapter::class, [ '_getMasterAdapter', '_executeMaster' ] );
    $pdo                 = $this->_getDisabledMock( \PDO::class, [ 'lastInsertId' ] );
    $statement           = $this->_getDisabledMock( \PDOStatement::class );
    $expected_column_sql = '(`' . implode( '`, `', array_keys( $insert_data ) ) . '`)';

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $adapter->expects( $this->once() )
      ->method( '_executeMaster' )
      ->with( $this->_table, $this->stringContains( "INSERT INTO `{$this->_table}` {$expected_column_sql}" ), $this->isType( 'array' ) )
      ->will( $this->returnValue( $statement ) );

    $pdo->expects( $this->once() )
      ->method( 'lastInsertId' )
      ->will( $this->returnValue( $insert_id ) );

    $this->assertEquals( $insert_id, $adapter->insert( $this->_table, $insert_data ) );

  } // insert


  /**
   * @test
   */
  public function insertNonIntegerKey() {

    $insert_data         = [ 'key' => 'value', 'type' => 'value' ];
    $adapter             = $this->_getDisabledMock( PdoAdapter::class, [ '_getMasterAdapter', '_executeMaster' ] );
    $pdo                 = $this->_getDisabledMock( \PDO::class, [ 'lastInsertId' ] );
    $statement           = $this->_getDisabledMock( \PDOStatement::class, [ 'rowCount' ] );
    $expected_column_sql = '(`' . implode( '`, `', array_keys( $insert_data ) ) . '`)';

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $adapter->expects( $this->once() )
      ->method( '_executeMaster' )
      ->with( $this->_table, $this->stringContains( "INSERT INTO `{$this->_table}` {$expected_column_sql}" ), $this->isType( 'array' ) )
      ->will( $this->returnValue( $statement ) );

    $pdo->expects( $this->once() )
      ->method( 'lastInsertId' )
      ->will( $this->returnValue( '0' ) );

    $statement->expects( $this->once() )
      ->method( 'rowCount' )
      ->will( $this->returnValue( 1 ) );

    $this->assertEquals( 1, $adapter->insert( $this->_table, $insert_data ) );

  } // insertNonIntegerKey


  /**
   * @return array
   */
  public function insertDataProvider() {

    $extra_data = $this->_insert_data;

    $extra_data['created_on']  = new Sql( 'NOW()' );
    $extra_data['modified_on'] = new Sql( 'NOW()' );

    return [
        'Without SQL'          => [ $this->_insert_data ],
        'With'                 => [ $extra_data ],
        'Keyword Column Names' => [ [ 'key' => 'value', 'type' => 'value' ] ],
    ];

  } // insertDataProvider


  /**
   * @test
   * @dataProvider fetchOneResults
   */
  public function fetchOne( $results, $expected, $master ) {

    $sql    = "SELECT value FROM anywhere";
    $params = [ 1, 2, 3 ];
    $target = $this->_setupFetch( $sql, $params, $results, $master );

    $this->assertSame( $expected, $target->fetchOne( $sql, $params, $master ) );

  } // fetchOne


  /**
   * @test
   * @dataProvider fetchRowResults
   */
  public function fetchRow( $results, $expected, $master ) {

    $sql    = "SELECT * FROM anywhere";
    $params = [ 1, 2, 3 ];
    $target = $this->_setupFetch( $sql, $params, $results, $master );

    $this->assertSame( $expected, $target->fetchRow( $sql, $params, $master ) );

  } // fetchRow


  /**
   * @test
   * @dataProvider fetchColResults
   */
  public function fetchCol( $results, $expected, $master ) {

    $sql    = "SELECT id FROM anywhere";
    $params = [ 1, 2, 3 ];
    $target = $this->_setupFetch( $sql, $params, $results, $master );

    $this->assertSame( $expected, $target->fetchCol( $sql, $params, $master ) );

  } // fetchCol


  /**
   * @test
   * @dataProvider fetchAllResults
   */
  public function fetchAll( $results, $expected, $master ) {

    $sql    = "SELECT * FROM anywhere";
    $params = [ 1, 2, 3 ];
    $target = $this->_setupFetch( $sql, $params, $results, $master );

    $this->assertSame( $expected, $target->fetchAll( $sql, $params, $master ) );

  } // fetchAll


  /**
   * @test
   * @dataProvider fetchAssocResults
   */
  public function fetchAssoc( $results, $expected, $master ) {

    $sql    = "SELECT * FROM anywhere";
    $params = [ 1, 2, 3 ];
    $target = $this->_setupFetch( $sql, $params, $results, $master );

    $this->assertSame( $expected, $target->fetchAssoc( $sql, $params, $master ) );

  } // fetchAssoc


  /**
   * @test
   * @dataProvider fetchPairsResults
   */
  public function fetchPairs( $results, $expected, $master ) {

    $sql    = "SELECT id, value FROM anywhere";
    $params = [ 1, 2, 3 ];
    $target = $this->_setupFetch( $sql, $params, $results, $master );

    $this->assertSame( $expected, $target->fetchPairs( $sql, $params, $master ) );

  } // fetchPairs


  /**
   * @test
   * @dataProvider fetchPairsBadResults
   * @expectedException Behance\NBD\Dbal\Exceptions\QueryRequirementException
   */
  public function fetchPairsBad( $results, $master ) {

    $sql    = "SELECT id, value FROM anywhere";
    $params = [ 1, 2, 3 ];
    $target = $this->_setupFetch( $sql, $params, $results, $master );

    $target->fetchPairs( $sql, $params, $master );

  } // fetchPairsBad


  /**
   * @return array
   */
  public function fetchPairsBadResults() {

    $rows1 = [
        [ 'value' => 789 ],
    ];


    $rows2 = [
        [ 'id' => 123 ],
        [ 'id' => 456 ]
    ];

    return [
        [ $rows1, false ],
        [ $rows1, true ],
        [ $rows2, false ],
        [ $rows2, true ],
    ];

  } // fetchPairsBadResults


  /**
   * @return array
   */
  public function fetchOneResults() {

    $value    = 123;
    $one_col  = [ [ 'a' => $value ] ];
    $two_col  = [ [ 'a' => $value, 'b' => 456 ] ];
    $two_rows = [ [ 'abc' => $value ], [ 'def' => 456 ] ];
    $zero     = [ [ 0 => 0 ] ];
    $empty    = [ [] ];

    return [
        [ $one_col, $value, false ],
        [ $one_col, $value, true ],
        [ $two_col, $value, false ],
        [ $two_col, $value, true ],
        [ $two_rows, $value, false ],
        [ $two_rows, $value, true ],
        [ $zero, 0, false ],
        [ $zero, 0, true ],
        [ $empty, null, false ],
        [ $empty, null, true ],
    ];

  } // fetchOneResults


  /**
   * @return array
   */
  public function fetchRowResults() {

    $one_col  = [ 'abc' => 123 ];
    $two_col  = [ 'abc' => 123, 'def' => 456 ];
    $two_rows = [ $one_col, [ 'def' => 456 ] ];
    $empty    = [ [] ];

    return [
        [ [ $one_col ], $one_col, false ],
        [ [ $one_col ], $one_col, true ],
        [ [ $two_col ], $two_col, false ],
        [ [ $two_col ], $two_col, true ],
        [ $two_rows, $one_col, false ],
        [ $two_rows, $one_col, true ],
        [ $empty, [], false ],
        [ $empty, [], true ],
    ];

  } // fetchRowResults


  /**
   * @return array
   */
  public function fetchColResults() {

    $value1 = 123;
    $value2 = 789;
    $values = [ $value1, $value2 ];


    $row1col1 = [ 'abc' => $value1 ];
    $row1col2 = [ 'def' => 456 ];

    $row2col1 = [ 'abc' => $value2 ];
    $row2col2 = [ 'def' => 101112 ];

    $row1 = array_merge( $row1col1, $row1col2 );
    $row2 = array_merge( $row2col1, $row2col2 );

    $two_rows = [ $row1, $row2 ];
    $empty    = [ [] ];

    return [
        [ [ $row1col1 ], [ $value1 ], false ],
        [ [ $row1col1 ], [ $value1 ], true ],
        [ [ $row1 ], [ $value1 ], false ],
        [ [ $row1 ], [ $value1 ], true ],
        [ $two_rows, $values, false ],
        [ $two_rows, $values, true ],
        [ $empty, [], false ],
        [ $empty, [], true ],
    ];

  } // fetchColResults


  /**
   * @return array
   */
  public function fetchAllResults() {

    $value1 = 123;
    $value2 = 789;

    $row1col1 = [ 'abc' => $value1 ];
    $row1col2 = [ 'def' => 456 ];

    $row2col1 = [ 'abc' => $value2 ];
    $row2col2 = [ 'def' => 101112 ];

    $row1 = array_merge( $row1col1, $row1col2 );
    $row2 = array_merge( $row2col1, $row2col2 );

    $two_rows = [ $row1, $row2 ];
    $empty    = [];

    return [
        [ [ $row1col1 ], [ $row1col1 ], false ],
        [ [ $row1col1 ], [ $row1col1 ], true ],
        [ [ $row1 ], [ $row1 ], false ],
        [ [ $row1 ], [ $row1 ], true ],
        [ $two_rows, $two_rows, false ],
        [ $two_rows, $two_rows, true ],
        [ $empty, $empty, false ],
        [ $empty, $empty, true ],
    ];

  } // fetchAllResults


  /**
   * @return array
   */
  public function fetchAssocResults() {

    $value1 = 123;
    $value2 = 789;

    $row1col1 = [ 'abc' => $value1 ];
    $row1col2 = [ 'def' => 456 ];

    $row2col1 = [ 'abc' => $value2 ];
    $row2col2 = [ 'def' => 101112 ];

    $row1     = array_merge( $row1col1, $row1col2 );
    $row2     = array_merge( $row2col1, $row2col2 );
    $two_rows = [ $row1, $row2 ];

    $empty = [];

    return [
        [ [ $row1col1 ], [ $value1 => $row1col1 ], false ],
        [ [ $row1col1 ], [ $value1 => $row1col1 ], true ],
        [ [ $row1 ], [ $value1 => $row1 ], false ],
        [ [ $row1 ], [ $value1 => $row1 ], true ],
        [ $two_rows, [ $value1 => $row1, $value2 => $row2 ], false ],
        [ $two_rows, [ $value1 => $row1, $value2 => $row2 ], true ],
        [ $empty, $empty, false ],
        [ $empty, $empty, true ],
    ];

  } // fetchAssocResults


  /**
   * @return array
   */
  public function fetchPairsResults() {

    $value1 = 123;
    $value2 = 456;
    $value3 = 789;
    $value4 = 101112;

    $rows1 = [
        [ 'id' => $value1, 'value' => $value2 ],
    ];

    $expected1 = [
        $value1 => $value2,
    ];

    $rows2 = [
        [ 'id' => $value1, 'value' => $value2 ],
        [ 'id' => $value3, 'value' => $value4 ]
    ];

    $rows2b = [
        [ 'id' => $value1, 'value' => $value2, 'extra' => 'abcdefg' ],
        [ 'id' => $value3, 'value' => $value4, 'extra' => 'hijklmn' ]
    ];

    $expected2 = [
        $value1 => $value2,
        $value3 => $value4
    ];

    return [
        [ $rows1,  $expected1, false ],
        [ $rows1,  $expected1, true ],
        [ $rows2,  $expected2, false ],
        [ $rows2,  $expected2, true ],
        [ $rows2b, $expected2, false ], // Ensure extra column is dropped
        [ $rows2b, $expected2, true ],
        [ [ [] ],  [], false ],
        [ [ [] ],  [], true ],
    ];

  } // fetchPairsResults


  /**
   * @param string $sql
   * @param array  $params
   * @param mixed  $results
   * @param bool   $master
   *
   * @return mock
   */
  private function _setupFetch( $sql, array $params, $results, $master ) {

    $target  = $this->_buildAdapter( null, [ 'query', 'queryMaster' ] );
    $adapter = new PPDO();
    $adapter->mock( $sql, $results, $params );

    $statement = $adapter->prepare( $sql );
    $statement->execute( $params );

    $target->expects( $this->once() )
      ->method( 'query' )
      ->with( $sql, $params, $master )
      ->will( $this->returnValue( $statement ) );

    return $target;

  } // _setupFetch


  /**
   * @return array
   */
  public function updateDataProvider() {

    $extra_data = $this->_update_data;

    $extra_data['modified_on'] = new Sql( 'NOW()' );

    return [
        'Without SQL' => [ $this->_update_data ],
        'With'        => [ $extra_data ]
    ];

  } // updateDataProvider


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function insertNonAssociative() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class );

    $adapter->insert( $this->_table, [ 'apples', 'oranges', 'peaches' ] );

  } // insertNonAssociative


  /**
   * Proves the ->insert() interface remains intact
   *
   * @test
   */
  public function insertIgnore() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class, [ 'insert' ] );
    $result  = '12345';

    $adapter->expects( $this->once() )
      ->method( 'insert' )
      ->with( $this->_table, $this->_insert_data, [ 'ignore' => true ] )
      ->will( $this->returnValue( $result ) );

    $this->assertSame( $result, $adapter->insertIgnore( $this->_table, $this->_insert_data ) );

  } // insertIgnore


  /**
   * Ensures that unmocked version of InsertIgnore object is processed correctly
   *
   * @test
   * @dataProvider boolProvider
   */
  public function insertIgnoreRaw( $ignored ) {

    $insert_id = '12345';

    $adapter   = $this->_getDisabledMock( PdoAdapter::class, [ '_getMasterAdapter', '_executeMaster' ] );
    $pdo       = $this->_getDisabledMock( \PDO::class, [ 'lastInsertId' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class );
    $insert_id = ( $ignored )
                 ? false
                 : $insert_id;

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $adapter->expects( $this->once() )
      ->method( '_executeMaster' )
      ->with( $this->_table, $this->stringContains( "INSERT IGNORE INTO `{$this->_table}`" ), $this->isType( 'array' ) )
      ->will( $this->returnValue( $statement ) );

    $pdo->expects( $this->once() )
      ->method( 'lastInsertId' )
      ->will( $this->returnValue( $insert_id ) );

    $expected = ( $ignored )
                ? 0
                : $insert_id;

    $this->assertEquals( $expected, $adapter->insertIgnore( $this->_table, $this->_insert_data ) );

  } // insertIgnoreRaw


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\QueryRequirementException
   */
  public function insertOnDuplicateNoUpdate() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class );

    $adapter->insertOnDuplicateUpdate( $this->_table, $this->_insert_data, [] );

  } // insertOnDuplicateNoUpdate


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function insertOnDuplicateIncorrectData() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class );

    $adapter->insertOnDuplicateUpdate( $this->_table, $this->_insert_data, [ 'foo' ] );

  } // insertOnDuplicateIncorrectData

  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function insertOnDuplicateIncorrectObject() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class );

    $adapter->insertOnDuplicateUpdate( $this->_table, $this->_insert_data, [ 'foo' => new \stdClass ] );

  } // insertOnDuplicateIncorrectObject


  /**
   * @test
   */
  public function insertOnDuplicateUpdate() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class, [ 'insert' ] );
    $result  = '12345';

    $adapter->expects( $this->once() )
      ->method( 'insert' )
      ->with( $this->_table, $this->_insert_data, [ 'on_duplicate' => $this->_update_data ] )
      ->will( $this->returnValue( $result ) );

    $this->assertSame( $result, $adapter->insertOnDuplicateUpdate( $this->_table, $this->_insert_data, $this->_update_data ) );

  } // insertOnDuplicateUpdate


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function insertOnDuplicateUpdateRaw( $inserted ) {

    $adapter   = $this->_getDisabledMock( PdoAdapter::class, [ '_execute', '_getMasterAdapter' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class );
    $pdo       = $this->_getDisabledMock( \PDO::class, [ 'lastInsertId' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'rowCount' ] );
    $result    = ( $inserted )
                 ? '12345' // Last insert ID
                 : 2;    // Returned by mysql on update

    if ( !$inserted ) {

      $statement->expects( $this->once() )
        ->method( 'rowCount' )
        ->will( $this->returnValue( $result ) );

    } // if inserted

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( $this->_table, $this->stringContains( "INSERT INTO" ), $this->isType( 'array' ), true )
      ->will( $this->returnValue( $statement ) );

    $last_id = ( $inserted )
               ? $result
               : '0';

    $pdo->expects( $this->once() )
      ->method( 'lastInsertId' )
      ->will( $this->returnValue( $last_id ) );

    $this->assertSame( $result, $adapter->insertOnDuplicateUpdate( $this->_table, $this->_insert_data, $this->_update_data ) );

  } // insertOnDuplicateUpdateRaw


  /**
   * @test
   */
  public function insertOnDuplicateUpdateSameData() {

    $adapter   = $this->_getDisabledMock( PdoAdapter::class, [ '_execute', '_getMasterAdapter' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class );
    $pdo       = $this->_getDisabledMock( \PDO::class, [ 'lastInsertId' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'rowCount' ] );

    $statement->expects( $this->once() )
      ->method( 'rowCount' )
      ->will( $this->returnValue( 0 ) );

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( $this->_table, $this->stringContains( "INSERT INTO" ), $this->isType( 'array' ), true )
      ->will( $this->returnValue( $statement ) );

    $pdo->expects( $this->once() )
      ->method( 'lastInsertId' )
      ->will( $this->returnValue( '0' ) );

    $this->assertSame( 0, $adapter->insertOnDuplicateUpdate( $this->_table, $this->_insert_data, $this->_update_data ) );

  } // insertOnDuplicateUpdateSameData

  /**
   * @test
   * @dataProvider boolProvider
   */
  public function insertOnDuplicateUpdateRawNonIntegerKey( $inserted ) {

    $adapter   = $this->_getDisabledMock( PdoAdapter::class, [ '_execute', '_getMasterAdapter' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class );
    $pdo       = $this->_getDisabledMock( \PDO::class, [ 'lastInsertId' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'rowCount' ] );
    $result    = ( $inserted )
                 ? 1  // Row inserted
                 : 2; // Row updated

    $statement->expects( $this->once() )
      ->method( 'rowCount' )
      ->will( $this->returnValue( $result ) );

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( $this->_table, $this->stringContains( "INSERT INTO" ), $this->isType( 'array' ), true )
      ->will( $this->returnValue( $statement ) );

    $last_id = '0';

    $pdo->expects( $this->once() )
      ->method( 'lastInsertId' )
      ->will( $this->returnValue( $last_id ) );

    $this->assertSame( $result, $adapter->insertOnDuplicateUpdate( $this->_table, $this->_insert_data, $this->_update_data ) );

  } // insertOnDuplicateUpdateRawNonIntegerKey


  /**
   * @test
   * @dataProvider getMethodsProvider
   */
  public function getWithoutColumn( $method, $resultset, $expected, $master ) {

    $statement = $this->_buildStatement( $resultset );
    $adapter   = $this->_buildAdapter( null, [ 'prepStarQuery', 'queryTable' ] );

    $built_sql    = $this->_sample_sql;
    $built_params = array_values( $this->_sample_where );

    $adapter->expects( $this->once() )
      ->method( 'prepStarQuery' )
      ->with( $this->_table, $this->_sample_where )
      ->will( $this->returnValue( [ $built_sql, $built_params ] ) );

    $adapter->expects( $this->once() )
      ->method( 'queryTable' )
      ->with( $this->_table, $built_sql, $built_params, $master )
      ->will( $this->returnValue( $statement ) );

    $results = $adapter->$method( $this->_table, $this->_sample_where, $master );

    $this->assertEquals( $expected, $results );

  } // getWithoutColumn


  /**
   * @return array
   */
  public function getMethodsProvider() {

    $resultset = $this->_sample_resultset;
    $assoc     = [];

    // Has the result set always keyed by first column
    $assoc[ $resultset[0]['id'] ] = $resultset[0];
    $assoc[ $resultset[1]['id'] ] = $resultset[1];

    return [
        // With multiple fields in fake result set, will always returns the first row
        'row master'     => [ 'getRow', $resultset, $resultset[0], true ],
        'row non-master' => [ 'getRow', $resultset, $resultset[0], false ],
        'row empty'      => [ 'getRow', [], [], false ],

        // The most "basic" - no transformation necessary
        'all master'     => [ 'getAll', $resultset, $resultset, true ],
        'all non-master' => [ 'getAll', $resultset, $resultset, false ],
        'all empty'      => [ 'getAll', [], [], false ],

        // Has the result set always keyed by first column
        'assoc master'     => [ 'getAssoc', $resultset, $assoc, true ],
        'assoc non-master' => [ 'getAssoc', $resultset, $assoc, false ],
        'assoc empty'      => [ 'getAssoc', [], [], false ],
    ];

  } // getMethodsProvider


  /**
   * @test
   * @dataProvider columnGetMethodsProvider
   */
  public function getWithColumn( $method, $resultset, $expected, $master ) {

    $statement = $this->_buildStatement( $resultset );
    $adapter   = $this->_buildAdapter( null, [ 'prepSelectorQuery', 'queryTable' ] );

    $built_sql    = $this->_sample_sql;
    $built_params = array_values( $this->_sample_where );

    $adapter->expects( $this->once() )
      ->method( 'prepSelectorQuery' )
      ->with( $this->_table, $this->_sample_column, $this->_sample_where )
      ->will( $this->returnValue( [ $built_sql, $built_params ] ) );

    $adapter->expects( $this->once() )
      ->method( 'queryTable' )
      ->with( $this->_table, $built_sql, $built_params, $master )
      ->will( $this->returnValue( $statement ) );

    $results = $adapter->$method( $this->_table, $this->_sample_column, $this->_sample_where, $master );

    $this->assertEquals( $expected, $results );

  } // getWithColumn


  /**
   * @return array
   */
  public function columnGetMethodsProvider() {

    $resultset = $this->_sample_resultset;
    $column    = [];

    // With multiple fields in fake result set, will always returns the first column
    foreach ( $resultset as $row ) {
      $column[] = $row['id'];
    }

    return [
        // With multiple fields in fake result set, will always returns the first value
        'one master'     => [ 'getOne', $resultset, $resultset[0]['id'], true ],
        'one non-master' => [ 'getOne', $resultset, $resultset[0]['id'], false ],
        'one empty'      => [ 'getOne', [], null, false ],
        // With multiple fields in fake result set, will always returns the first column
        'col master'     => [ 'getColumn', $resultset, $column, true ],
        'col non-master' => [ 'getColumn', $resultset, $column, false ],
        'col empty'      => [ 'getColumn', [], [], false ],
    ];

  } // columnGetMethodsProvider


  /**
   * @test
   */
  public function quote() {

    $value  = 'won\'t matter';
    $result = "won\\'t matter";

    $connection = $this->_getDisabledMock( ConnectionService::class, [ 'getMaster' ] );
    $pdo        = $this->_getDisabledMock( \PDO::class, [ 'quote' ] );
    $adapter    = $this->getMockBuilder( PdoAdapter::class )
      ->setMethods( [ '_getReplicaAdapter' ] )
      ->setConstructorArgs( [ $connection ] )
      ->getMock();

    $pdo->expects( $this->once() )
      ->method( 'quote' )
      ->with( $value )
      ->will( $this->returnValue( $result ) );

    $adapter->expects( $this->once() )
      ->method( '_getReplicaAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $this->assertSame( $result, $adapter->quote( $value ) );

  } // quote


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function updateNoData() {

    $this->_buildAdapter()->update( 'abc', [], 'abc=1' );

  } // updateNoData


  /**
   * @test
   * @expectedException Behance\NBD\Dbal\Exceptions\QueryRequirementException
   */
  public function updateNoWhere() {

    $connection = $this->_getDisabledMock( ConnectionService::class );
    $adapter    = new PdoAdapter( $connection );
    $adapter->update( 'abc', [ 'xyz' => 123 ], '' );

  } // updateNoWhere


  /**
   * @test
   * @dataProvider badWhereProvider
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function updateBadWhere( $where ) {

    $connection = $this->_getDisabledMock( ConnectionService::class );
    $adapter    = new PdoAdapter( $connection );
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
  public function updateWhere( $where ) {

    $affected  = 1;
    $adapter   = $this->_getDisabledMock( PdoAdapter::class, [ '_executeMaster' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'rowCount' ] );

    $adapter->expects( $this->once() )
      ->method( '_executeMaster' )
      ->with( $this->_table, $this->stringContains( "UPDATE `{$this->_table}` SET " ) )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'rowCount' )
      ->will( $this->returnValue( $affected ) );

    $this->assertSame( $affected, $adapter->update( $this->_table, $this->_update_data, $where ) );

  } // updateWhere


  /**
   * @test
   * @dataProvider updateDataProvider
   */
  public function update( $update_data ) {

    $affected  = 1;
    $adapter   = $this->_getDisabledMock( PdoAdapter::class, [ '_executeMaster' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'rowCount' ] );

    $adapter->expects( $this->once() )
      ->method( '_executeMaster' )
      ->with( $this->_table, $this->stringContains( "UPDATE `{$this->_table}` SET" ) )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'rowCount' )
      ->will( $this->returnValue( $affected ) );

    $this->assertSame( $affected, $adapter->update( $this->_table, $update_data, [ 'id' => 5 ] ) );

  } // update


  /**
   * @test
   * @dataProvider whereProvider
   */
  public function delete( $where ) {

    $affected  = 1;
    $adapter   = $this->_getDisabledMock( PdoAdapter::class, [ '_executeMaster' ] );
    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'rowCount' ] );

    $adapter->expects( $this->once() )
      ->method( '_executeMaster' )
      ->with( $this->_table, $this->stringContains( "DELETE FROM `{$this->_table}` WHERE " ) )
      ->will( $this->returnValue( $statement ) );

    $statement->expects( $this->once() )
      ->method( 'rowCount' )
      ->will( $this->returnValue( $affected ) );

    $this->assertSame( $affected, $adapter->delete( $this->_table, $where ) );

  } // delete


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

    $connection = $this->_getDisabledMock( ConnectionService::class );
    $adapter    = new PdoAdapter( $connection );
    $adapter->delete( 'abc', '' );

  } // deleteNoWhere


  /**
   * @test
   * @dataProvider badWhereProvider
   * @expectedException Behance\NBD\Dbal\Exceptions\InvalidQueryException
   */
  public function deleteBadWhere( $where ) {

    $connection = $this->_getDisabledMock( ConnectionService::class );
    $adapter    = new PdoAdapter( $connection );
    $adapter->delete( 'abc', $where );

  } // deleteBadWhere


  /**
   * @test
   */
  public function beginTransaction() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class, [ '_getMasterAdapter' ] );
    $pdo     = $this->_getDisabledMock( PDO::class, [ 'beginTransaction' ] );

    $adapter->expects( $this->once() )
      ->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $pdo->expects( $this->once() )
      ->method( 'beginTransaction' )
      ->will( $this->returnValue( true ) );

    $this->assertTrue( $adapter->beginTransaction() );
    $this->assertTrue( $adapter->isInTransaction() );

  } // beginTransaction


  /**
   * @test
   */
  public function commit() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class, [ '_getMasterAdapter' ] );
    $pdo     = $this->_getDisabledMock( PDO::class, [ 'commit', 'beginTransaction' ] );

    $adapter->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $pdo->expects( $this->once() )
      ->method( 'commit' )
      ->will( $this->returnValue( true ) );

    $adapter->beginTransaction();

    $this->assertTrue( $adapter->isInTransaction() );
    $this->assertTrue( $adapter->commit() );
    $this->assertFalse( $adapter->isInTransaction() );

  } // commit


  /**
   * @test
   */
  public function rollBack() {

    $adapter = $this->_getDisabledMock( PdoAdapter::class, [ '_getMasterAdapter' ] );
    $pdo     = $this->_getDisabledMock( PDO::class, [ 'rollBack', 'beginTransaction' ] );

    $adapter->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $pdo->expects( $this->once() )
      ->method( 'rollBack' )
      ->will( $this->returnValue( true ) );

    $adapter->beginTransaction();

    $this->assertTrue( $adapter->isInTransaction() );
    $this->assertTrue( $adapter->rollBack() );
    $this->assertFalse( $adapter->isInTransaction() );

  } // rollBack


  /**
   * @test
   */
  public function closeConnection() {

    $connection = $this->_getDisabledMock( ConnectionService::class, [ 'closeOpenedConnections' ] );
    $pdo        = $this->_getDisabledMock( PDO::class, [ 'beginTransaction' ] );
    $adapter    = $this->getMockBuilder( PdoAdapter::class )
      ->setMethods( [ '_getMasterAdapter' ] )
      ->setConstructorArgs( [ $connection ] )
      ->getMock();

    $connection->expects( $this->once() )
      ->method( 'closeOpenedConnections' );

    $adapter->method( '_getMasterAdapter' )
      ->will( $this->returnValue( $pdo ) );

    $adapter->beginTransaction();

    $this->assertTrue( $adapter->isInTransaction() );

    $adapter->closeConnection();

    $this->assertFalse( $adapter->isInTransaction() );

  } // closeConnection


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function prepStarQuery( $empty_params ) {

    $adapter = $this->_buildAdapter( null, [ '_buildWhere', '_quoteTable' ] );

    $params  = ( $empty_params )
               ? []
               : [ 123 ];

    $prepared_where = [ 'WHERE abc=?', $params ];
    $quoted_table   = '`' . $this->_table . '`';
    $adapter->expects( $this->once() )
      ->method( '_buildWhere' )
      ->with( $this->_sample_where )
      ->will( $this->returnValue( $prepared_where ) );

    $adapter->expects( $this->once() )
      ->method( '_quoteTable' )
      ->with( $this->_table )
      ->will( $this->returnValue( $quoted_table ) );

    $results = $adapter->prepStarQuery( $this->_table, $this->_sample_where );

    // NOTE: asserting the actual generated SQL, with caveats for mocked functions
    $expected_sql    = "SELECT * FROM {$quoted_table} " . $prepared_where[0];
    $expected_params = ( $empty_params )
                       ? null
                       : $prepared_where[1];

    $this->assertEquals( [ $expected_sql, $expected_params ], $results );

  } // prepStarQuery


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function prepSelectorQuery( $empty_params ) {

    $adapter = $this->_buildAdapter( null, [ '_buildWhere', '_quoteTable', '_quoteColumn' ] );

    $params  = ( $empty_params )
               ? []
               : [ 123 ];

    $prepared_where = [ 'WHERE abc=?', $params ];
    $quoted_table   = '`' . $this->_table . '`';
    $quoted_column  =  '`' . $this->_sample_column . '`';

    $adapter->expects( $this->once() )
      ->method( '_buildWhere' )
      ->with( $this->_sample_where )
      ->will( $this->returnValue( $prepared_where ) );

    $adapter->expects( $this->once() )
      ->method( '_quoteTable' )
      ->with( $this->_table )
      ->will( $this->returnValue( $quoted_table ) );

    $adapter->expects( $this->once() )
      ->method( '_quoteColumn' )
      ->with( $this->_sample_column )
      ->will( $this->returnValue( $quoted_column ) );

    $results = $adapter->prepSelectorQuery( $this->_table, $this->_sample_column, $this->_sample_where );

    // NOTE: asserting the actual generated SQL, with caveats for mocked functions
    $expected_sql    = "SELECT {$quoted_column} FROM {$quoted_table} " . $prepared_where[0];
    $expected_params = ( $empty_params )
                       ? null
                       : $prepared_where[1];

    $this->assertEquals( [ $expected_sql, $expected_params ], $results );

  } // prepSelectorQuery


  /**
   * @test
   * @dataProvider oneValueResultsetProvider
   */
  public function extractOneValue( $resultset, $expected ) {

    $statement = $this->_buildStatement( $resultset );
    $adapter   = $this->_buildAdapter();

    $this->assertEquals( $expected, $adapter->extractOneValue( $statement ) );

  } // extractOneValue


  /**
   * @return array
   */
  public function oneValueResultsetProvider() {

    $multiple_rows = $this->_sample_resultset;
    $single_row    = [ $this->_sample_resultset[0] ];

    return [
        'no columns'    => [ [], null ],
        'empty results' => [ [ [], [] ], null ],
        'multiple rows' => [ $multiple_rows, $multiple_rows[0]['id'] ],
        'one row'       => [ $single_row, $single_row[0]['id'] ]
    ];

  } // oneValueResultsetProvider


  /**
   * @test
   */
  public function extractOneValueCorrupted() {

    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'fetch' ] );

    $statement->expects( $this->once() )
      ->method( 'fetch' )
      ->will( $this->returnValue( false ) );

    $this->assertNull( $this->_buildAdapter()->extractOneValue( $statement ) );

  } // extractOneValueCorrupted

  /**
   * @test
   * @dataProvider keyValuesResultsetProvider
   */
  public function extractKeyValues( $resultset, $expected ) {

    $statement = $this->_buildStatement( $resultset );
    $adapter   = $this->_buildAdapter();

    $this->assertEquals( $expected, $adapter->extractKeyValues( $statement ) );

  } // extractKeyValues

  /**
   * @return array
   */
  public function keyValuesResultsetProvider() {

    $multiple_rows = $this->_sample_resultset;
    $single_row    = [ $this->_sample_resultset[0] ];

    return [
        'no columns'    => [ [], [] ],
        'empty results' => [ [ [], [] ], [] ],
        'multiple rows' => [ $multiple_rows, $multiple_rows[0] ],
        'one row'       => [ $single_row, $single_row[0] ]
    ];

  } // keyValuesResultsetProvider


  /**
   * @test
   */
  public function extractKeyValuesCorrupted() {

    $statement = $this->_getDisabledMock( \PDOStatement::class, [ 'fetch' ] );

    $statement->expects( $this->once() )
      ->method( 'fetch' )
      ->will( $this->returnValue( false ) );

    $this->assertEquals( [], $this->_buildAdapter()->extractKeyValues( $statement ) );

  } // extractKeyValuesCorrupted


  /**
   * @test
   * @dataProvider arrayValuesResultsetProvider
   */
  public function extractArrayValues( $resultset, $expected ) {

    $statement = $this->_buildStatement( $resultset );
    $adapter   = $this->_buildAdapter();

    $this->assertEquals( $expected, $adapter->extractArrayValues( $statement ) );

  } // extractArrayValues


  /**
   * @return array
   */
  public function arrayValuesResultsetProvider() {

    $multiple_rows = $this->_sample_resultset;
    $single_row    = [ $this->_sample_resultset[0] ];

    $column = [];
    foreach ( $multiple_rows as $row ) {
      $column[] = $row['id'];
    }

    return [
        'no columns'    => [ [], [] ],
        'empty results' => [ [ [], [] ], [] ],
        'multiple rows' => [ $multiple_rows, $column ],
        'one row'       => [ $single_row, [ $single_row[0]['id'] ] ]
    ];

  } // arrayValuesResultsetProvider


  /**
   * @test
   * @dataProvider assocResultsetProvider
   */
  public function extractAssocValues( $resultset, $expected ) {

    $statement = $this->_buildStatement( $resultset );
    $adapter   = $this->_buildAdapter();

    $this->assertEquals( $expected, $adapter->extractAssocValues( $statement ) );

  } // extractAssocValues


  /**
   * @return array
   */
  public function assocResultsetProvider() {

    $multiple_rows = $this->_sample_resultset;
    $single_row    = [ $this->_sample_resultset[0] ];

    return [
        'no columns'    => [ [], [] ],
        'empty results' => [ [ [], [] ], [] ],
        'multiple rows' => [ $multiple_rows, $multiple_rows ],
        'one row'       => [ $single_row, $single_row ]
    ];

  } // assocResultsetProvider


  /**
   * @test
   * @dataProvider columnAssocValuesProvider
   */
  public function extractColumnAssocValues( $resultset, $expected ) {

    $statement = $this->_buildStatement( $resultset );
    $adapter   = $this->_buildAdapter();

    $this->assertEquals( $expected, $adapter->extractColumnAssocValues( $statement ) );

  } // extractColumnAssocValues


  /**
   * @return array
   */
  public function columnAssocValuesProvider() {

    $multiple_rows = $this->_sample_resultset;
    $single_row    = [ $this->_sample_resultset[0] ];

    $result = [];
    foreach ( $multiple_rows as $row ) {
      $result[ $row['id'] ] = $row;
    }

    return [
        'no columns'    => [ [], [] ],
        'empty results' => [ [ [], [] ], [] ],
        'multiple rows' => [ $multiple_rows, $result ],
        'one row'       => [ $single_row, [ $single_row[0]['id'] => $single_row[0] ] ]
    ];

  } // columnAssocValuesProvider


  /**
   * @test
   * @dataProvider pairValuesProvider
   */
  public function extractPairValues( $resultset, $expected ) {

    $statement = $this->_buildStatement( $resultset );
    $adapter   = $this->_buildAdapter();

    $this->assertEquals( $expected, $adapter->extractPairValues( $statement ) );

  } // extractPairValues


  /**
   * @return array
   */
  public function pairValuesProvider() {

    $multiple_rows = $this->_sample_resultset;
    $single_row    = [ $this->_sample_resultset[0] ];

    $result = [];
    foreach ( $multiple_rows as $row ) {
      $result[ $row['id'] ] = $row['enabled'];
    }

    return [
        'no columns'    => [ [], [] ],
        'empty results' => [ [ [], [] ], [] ],
        'multiple rows' => [ $multiple_rows, $result ],
        'one row'       => [ $single_row, [ $single_row[0]['id'] => $single_row[0]['enabled'] ] ]
    ];

  } // pairValuesProvider


  /**
   * @param array $results
   *
   * @return PDOStatement
   */
  private function _buildStatement( $results ) {

    $p = new \Pseudo\Pdo();
    $p->mock( $this->_sample_sql, $results );

    return $p->query( $this->_sample_sql );

  } // _buildStatement


  /**
   * @param Behance\NBD\Dbal\ConnectionService $connection
   * @param array                              $functions
   *
   * @return Behance\NBD\Dbal\Adapters\PdoAdapter
   */
  private function _buildAdapter( ConnectionService $connection = null, array $functions = [] ) {

    $config     = $this->_getDisabledMock( ConfigService::class );
    $connection = $connection ?: $this->getMockBuilder( ConnectionService::class )
      ->setConstructorArgs( [ $config ] )
      // ->setMethods( [] )
      ->getMock();

    return $this->_getAbstractMock( PDOAdapter::class, $functions, [ $connection ] );

  } // _buildAdapter

} // PdoAdapterTest
