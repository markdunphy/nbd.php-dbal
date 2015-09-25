<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\Test\BaseTest;

use Pseudo\Pdo as PPDO;
use Symfony\Component\EventDispatcher\EventDispatcher;

/**
 * @covers Behance\NBD\Dbal\AdapterAbstract<extended>
 */
class AdapterAbstractTest extends BaseTest {

  private $_table = 'my_table';


  /**
   * @test
   */
  public function construct() {

    $connection = $this->_getDisabledMock( ConnectionService::class );
    $target     = $this->getMockForAbstractClass( AdapterAbstract::class, [ $connection ] );

    $this->assertSame( $connection, $target->getConnection() );

  } // construct


  /**
   * @test
   */
  public function closeConnection() {

    $result     = 1234;
    $connection = $this->_getDisabledMock( ConnectionService::class, [ 'closeOpenedConnections' ] );
    $target     = $this->getMockForAbstractClass( AdapterAbstract::class, [ $connection ] );

    $connection->expects( $this->once() )
      ->method( 'closeOpenedConnections' )
      ->will( $this->returnValue( $result ) );

    $this->assertSame( $result, $target->closeConnection() );

  } // closeConnection


  /**
   * @test
   */
  public function bindEvent() {

    $connection = $this->_getDisabledMock( ConnectionService::class );
    $dispatcher = $this->getMock( EventDispatcher::class, [ 'addListener' ] );
    $adapter    = $this->getMockForAbstractClass( AdapterAbstract::class, [ $connection, $dispatcher ] );

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
  public function fetchOneResults() {

    $value    = 123;
    $one_col  = [ [ 'a' => $value ] ];
    $two_col  = [ [ 'a' => $value, 'b' => 456 ] ];
    $two_rows = [ [ 'abc' => $value ], [ 'def' => 456 ] ];
    $empty    = [ [] ];

    return [
        [ $one_col, $value, false ],
        [ $one_col, $value, true ],
        [ $two_col, $value, false ],
        [ $two_col, $value, true ],
        [ $two_rows, $value, false ],
        [ $two_rows, $value, true ],
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

    $value1   = 123;
    $value2   = 789;

    $row1col1  = [ 'abc' => $value1 ];
    $row1col2  = [ 'def' => 456 ];

    $row2col1  = [ 'abc' => $value2 ];
    $row2col2  = [ 'def' => 101112 ];

    $row1  = array_merge( $row1col1, $row1col2 );
    $row2  = array_merge( $row2col1, $row2col2 );

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

    $value1    = 123;
    $value2    = 789;

    $row1col1  = [ 'abc' => $value1 ];
    $row1col2  = [ 'def' => 456 ];

    $row2col1  = [ 'abc' => $value2 ];
    $row2col2  = [ 'def' => 101112 ];

    $row1      = array_merge( $row1col1, $row1col2 );
    $row2      = array_merge( $row2col1, $row2col2 );
    $two_rows  = [ $row1, $row2 ];

    $empty     = [];

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
   * @test
   * @dataProvider getQueryProvider
   */
  public function getQueries( $source, $destination, $where, $sql, $master ) {

    $statement  = $this->_getDisabledMock( \PDOStatement::class );
    $connection = $this->_getDisabledMock( ConnectionService::class );
    $target     = $this->_getAbstractMock( AdapterAbstract::class, [ $destination ], [ $connection ] );

    $target->expects( $this->once() )
      ->method( $destination )
      ->with( $this->stringContains( $sql ), array_values( $where ), $master )
      ->will( $this->returnValue( $statement ) );

    $result = $target->$source( $this->_table, $where, $master );
    $this->assertInstanceOf( \PDOStatement::class, $result );

  } // getQueries


  /**
   * Ensures the many conditions of WHERE building are correct
   *
   * @test
   * @dataProvider getRowWhereProvider
   */
  public function getRowWhere( $where, $expected_sql, $expected_params ) {

    $statement  = $this->_getDisabledMock( \PDOStatement::class );
    $connection = $this->_getDisabledMock( ConnectionService::class );
    $target     = $this->_getAbstractMock( AdapterAbstract::class, [ 'fetchRow' ], [ $connection ] );

    $target->expects( $this->once() )
      ->method( 'fetchRow' )
      ->with( $this->stringContains( $expected_sql ), $expected_params )
      ->will( $this->returnValue( $statement ) );

    $result = $target->getRow( $this->_table, $where );
    $this->assertInstanceOf( \PDOStatement::class, $result );

  } // getRowWhere


  /**
   * @return array
   */
  public function getRowWhereProvider() {

    $base  = "SELECT * FROM `{$this->_table}`";

    $value1 = 123;
    $value2 = 456;

    $single = [ 'abc' => $value1 ];
    $double = [ 'abc' => $value1, 'def' => $value2 ];
    $triple = [ 'abc' => $value1, 'def' => $value2, 'time' => new Sql( 'NOW()' ) ];

    $string = "abc=123";
    $object  = new Sql( $string );
    $double_null = [ 'abc' => null, 'def' => $value2 ];

    return [
        [ '', $base, null ],
        [ $single, "`abc` = ?", [ $value1 ] ],
        [ $double, "`abc` = ? AND `def` = ?", [ $value1, $value2 ] ],
        [ $double_null, "`abc` IS NULL AND `def` = ?", [ $value2 ] ],
        [ $triple, "`abc` = ? AND `def` = ? AND `time` = NOW()", [ $value1, $value2 ] ],
        [ $string, $string, null ],
        [ $object, $string, null ],
    ];

  } // getRowWhereProvider


  /**
   * @test
   * @dataProvider getColumnQueryProvider
   */
  public function getColumnQueries( $source, $destination, $column, $where, $sql, $master ) {

    $statement  = $this->_getDisabledMock( \PDOStatement::class );
    $connection = $this->_getDisabledMock( ConnectionService::class );
    $target     = $this->_getAbstractMock( AdapterAbstract::class, [ $destination ], [ $connection ] );

    $where_values = ( empty( $where ) )
                    ? null
                    : array_values( $where );

    $target->expects( $this->once() )
      ->method( $destination )
      ->with( $this->stringContains( $sql ), $where_values, $master )
      ->will( $this->returnValue( $statement ) );

    $result = $target->$source( $this->_table, $column, $where, $master );
    $this->assertInstanceOf( \PDOStatement::class, $result );

  } // getColumnQueries


  /**
   * @return array
   */
  public function getQueryProvider() {

    $sql   = "SELECT * FROM `{$this->_table}` WHERE `abc` = ? AND `def` = ?";
    $where = [ 'abc' => 123, 'def' => 456 ];

    return [
        [ 'getRow', 'fetchRow', $where, $sql, false ],
        [ 'getRow', 'fetchRow', $where, $sql, true ],
        [ 'getAll', 'fetchAll', $where, $sql, false ],
        [ 'getAll', 'fetchAll', $where, $sql, true ],
        [ 'getAssoc', 'fetchAssoc', $where, $sql, false ],
        [ 'getAssoc', 'fetchAssoc', $where, $sql, true ],
    ];

  } // getQueryProvider


  /**
   * @return array
   */
  public function getColumnQueryProvider() {

    $column = 'enabled';
    $sql    = "SELECT `{$column}` FROM `{$this->_table}` WHERE `abc` = ? AND `def` = ?";
    $sql_nowhere = "SELECT `{$column}` FROM `{$this->_table}`";
    $where  = [ 'abc' => 123, 'def' => 456 ];

    return [
        [ 'getOne', 'fetchOne', $column, $where, $sql, false ],
        [ 'getOne', 'fetchOne', $column, $where, $sql, true ],
        [ 'getCol', 'fetchColumn', $column, $where, $sql, false ],
        [ 'getCol', 'fetchColumn', $column, $where, $sql, true ],
        [ 'getColumn', 'fetchColumn', $column, $where, $sql, false ],
        [ 'getColumn', 'fetchColumn', $column, $where, $sql, true ],
        [ 'getColumn', 'fetchColumn', $column, '', $sql_nowhere, false ],
        [ 'getColumn', 'fetchColumn', $column, '', $sql_nowhere, true ],
    ];

  } // getColumnQueryProvider


  /**
   * @param string $sql
   * @param array  $params
   * @param mixed  $results
   * @param bool   $master
   *
   * @return mock
   */
  private function _setupFetch( $sql, array $params, $results, $master ) {

    $connection = $this->_getDisabledMock( ConnectionService::class );
    $target     = $this->_getAbstractMock( AdapterAbstract::class, [ 'query', 'queryMaster' ], [ $connection ] );

    $adapter    = new PPDO();
    $adapter->mock( $sql, $results );

    $statement = $adapter->prepare( $sql );
    $statement->execute( $params );

    $target->expects( $this->once() )
      ->method( 'query' )
      ->with( $sql, $params, $master )
      ->will( $this->returnValue( $statement ) );

    return $target;

  } // _setupFetch

} // AdapterAbstractTest
