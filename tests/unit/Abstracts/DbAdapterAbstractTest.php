<?php

namespace Behance\NBD\Dbal\Abstracts;

use Behance\NBD\Dbal\Test\BaseTest;
use Symfony\Component\EventDispatcher\EventDispatcher;

use Zend\Db\ResultSet\ResultSet as ZendResultSet;

class DbAdapterAbstractTest extends BaseTest {

  private $_target             = 'Behance\NBD\Dbal\Abstracts\DbAdapterAbstract',
          $_connection_service = 'Behance\NBD\Dbal\Services\ConnectionService';

  /**
   * @test
   */
  public function construct() {

    $connection = $this->_getDisabledMock( $this->_connection_service );
    $dispatcher = new EventDispatcher();
    $adapter    = $this->getMockForAbstractClass( $this->_target, [ $connection, $dispatcher ] );

    $this->assertSame( $dispatcher, $adapter->getEventDispatcher() );
    $this->assertSame( $connection, $adapter->getConnection() );

  } // construct


  /**
   * @test
   * @dataProvider closableConnectionProvider
   */
  public function closeConnection( array $adapters ) {

    $connection = $this->_getDisabledMock( $this->_connection_service, [ 'getOpenedConnections' ] );
    $target     = $this->getMockForAbstractClass( $this->_target, [ $connection ] );

    $connection->expects( $this->once() )
      ->method( 'getOpenedConnections' )
      ->will( $this->returnValue( $adapters ) );

    $this->assertSame( count( $adapters ), $target->closeConnection() );

  } // closeConnection


  /**
   * @return array
   */
  public function closableConnectionProvider() {

    return [
        'None' => [ [] ],
        '1'    => [ [ $this->_createClosableAdapter() ] ],
        '2'    => [ [ $this->_createClosableAdapter(), $this->_createClosableAdapter() ] ]
    ];

  } // closableConnectionProvider


  /**
   * @test
   */
  public function bindEvent() {

    $connection = $this->_getDisabledMock( $this->_connection_service );
    $dispatcher = $this->getMock( 'Symfony\Component\EventDispatcher\EventDispatcher', [ 'addListener' ] );
    $adapter    = $this->getMockForAbstractClass( $this->_target, [ $connection, $dispatcher ] );

    $event_name = 'event.abcdef';
    $handler    = ( function() {
      return 123;
    } );

    $dispatcher->expects( $this->once() )
      ->method( 'addListener' )
      ->with( $event_name, $handler );

    $adapter->bindEvent( $event_name, $handler );

  } // bindEvent


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
  public function fetchOneResults() {

    $value    = 123;
    $one_col  = [ [ 'a' => $value ] ];
    $two_col  = [ [ 'a' => $value, 'b' => 456 ] ];
    $two_rows = [ [ 'abc' => $value ], [ 'def' => 456 ] ];
    $empty    = [ [] ];

    return [
        [ $this->_createResultSet( $one_col ), $value, false ],
        [ $this->_createResultSet( $one_col ), $value, true ],
        [ $this->_createResultSet( $two_col ), $value, false ],
        [ $this->_createResultSet( $two_col ), $value, true ],
        [ $this->_createResultSet( $two_rows ), $value, false ],
        [ $this->_createResultSet( $two_rows ), $value, true ],
        [ $this->_createResultSet( $empty ), null, false ],
        [ $this->_createResultSet( $empty ), null, true ],
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
        [ $this->_createResultSet( [ $one_col ] ), $one_col, false ],
        [ $this->_createResultSet( [ $one_col ] ), $one_col, true ],
        [ $this->_createResultSet( [ $two_col ] ), $two_col, false ],
        [ $this->_createResultSet( [ $two_col ] ), $two_col, true ],
        [ $this->_createResultSet( $two_rows ), $one_col, false ],
        [ $this->_createResultSet( $two_rows ), $one_col, true ],
        [ $this->_createResultSet( $empty ), [], false ],
        [ $this->_createResultSet( $empty ), [], true ],
    ];

  } // fetchRowResults


  /**
   * @return array
   */
  public function fetchColResults() {

    $value1   = 123;
    $value2   = 789;
    $values   = [ $value1, $value2 ];


    $row1col1  = [ 'abc' => $value1 ];
    $row1col2  = [ 'def' => 456 ];

    $row2col1  = [ 'abc' => $value2 ];
    $row2col2  = [ 'def' => 101112 ];

    $row1  = array_merge( $row1col1, $row1col2 );
    $row2  = array_merge( $row2col1, $row2col2 );

    $two_rows = [ $row1, $row2 ];
    $empty    = [ [] ];

    return [
        [ $this->_createResultSet( [ $row1col1 ] ), [ $value1 ], false ],
        [ $this->_createResultSet( [ $row1col1 ] ), [ $value1 ], true ],
        [ $this->_createResultSet( [ $row1 ] ), [ $value1 ], false ],
        [ $this->_createResultSet( [ $row1 ] ), [ $value1 ], true ],
        [ $this->_createResultSet( $two_rows ), $values, false ],
        [ $this->_createResultSet( $two_rows ), $values, true ],
        [ $this->_createResultSet( $empty ), [], false ],
        [ $this->_createResultSet( $empty ), [], true ],
    ];

  } // fetchColResults


  /**
   * @return array
   */
  public function fetchAllResults() {

    $value1   = 123;
    $value2   = 789;

    $row1col1  = [ 'abc' => $value1 ];
    $row1col2  = [ 'def' => 456 ];

    $row2col1  = [ 'abc' => $value2 ];
    $row2col2  = [ 'def' => 101112 ];

    $row1  = array_merge( $row1col1, $row1col2 );
    $row2  = array_merge( $row2col1, $row2col2 );

    $two_rows = [ $row1, $row2 ];
    $empty    = [ [] ];

    return [
        [ $this->_createResultSet( [ $row1col1 ] ), [ $row1col1 ], false ],
        [ $this->_createResultSet( [ $row1col1 ] ), [ $row1col1 ], true ],
        [ $this->_createResultSet( [ $row1 ] ), [ $row1 ], false ],
        [ $this->_createResultSet( [ $row1 ] ), [ $row1 ], true ],
        [ $this->_createResultSet( $two_rows ), $two_rows, false ],
        [ $this->_createResultSet( $two_rows ), $two_rows, true ],
        [ $this->_createResultSet( $empty ), $empty, false ],
        [ $this->_createResultSet( $empty ), $empty, true ],
    ];

  } // fetchAllResults


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
        [ $this->_createResultSet( $rows1 ),  $expected1, false ],
        [ $this->_createResultSet( $rows1 ),  $expected1, true ],
        [ $this->_createResultSet( $rows2 ),  $expected2, false ],
        [ $this->_createResultSet( $rows2 ),  $expected2, true ],
        [ $this->_createResultSet( $rows2b ), $expected2, false ], // Ensure extra column is dropped
        [ $this->_createResultSet( $rows2b ), $expected2, true ],
        [ $this->_createResultSet( [ [] ] ), [], false ],
        [ $this->_createResultSet( [ [] ] ), [], true ],
    ];

  } // fetchPairsResults


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
        [ $this->_createResultSet( $rows1 ), false ],
        [ $this->_createResultSet( $rows1 ), true ],
        [ $this->_createResultSet( $rows2 ), false ],
        [ $this->_createResultSet( $rows2 ), true ],
    ];

  } // fetchPairsBadResults


  /**
   * @param string $sql
   * @param array  $params
   * @param mixed  $results
   * @param bool   $master
   *
   * @return mock
   */
  private function _setupFetch( $sql, array $params, $results, $master ) {

    $connection = $this->_getDisabledMock( $this->_connection_service );
    $target     = $this->_getAbstractMock( $this->_target, [ 'query', 'queryMaster' ], [ $connection ] );

    if ( $master ) {

      $target->expects( $this->once() )
        ->method( 'queryMaster' )
        ->with( $sql, $params )
        ->will( $this->returnValue( $results ) );

      $target->expects( $this->never() )
        ->method( 'query' );

    } // if master

    else {

      $target->expects( $this->once() )
        ->method( 'query' )
        ->with( $sql, $params )
        ->will( $this->returnValue( $results ) );

      $target->expects( $this->never() )
        ->method( 'queryMaster' );

    } // else (!master)

    return $target;

  } // _setupFetch


  /**
   * @return Zend\Db\ResultSet\ResultSet
   */
  private function _createResultSet( $data ) {

    $result = new ZendResultSet( ZendResultSet::TYPE_ARRAY );

    $result->initialize( $data );

    return $result;

  } // _createResultSet

  /**
   * @return Zend_Db_Adapter_Adapter
   */
  private function _createClosableAdapter() {

    $mock    = $this->_getDisabledMock( 'Zend\Db\Adapter\Adapter', [ 'getDriver' ] );
    $driver  = $this->_getDisabledMock( 'Zend\Db\Adapter\Driver\Pdo\Pdo', [ 'getConnection' ] ) ;
    $connect = $this->_getDisabledMock( 'Zend\Db\Adapter\Driver\Pdo\Connection', [ 'disconnect' ] );

    $mock->expects( $this->once() )
      ->method( 'getDriver' )
      ->will( $this->returnValue( $driver ) );

    $driver->expects( $this->once() )
      ->method( 'getConnection' )
      ->will( $this->returnValue( $connect ) );

    $connect->expects( $this->once() )
      ->method( 'disconnect' );

    return $mock;

  } // _createClosableAdapter

} // DbAdapterAbstractTest
