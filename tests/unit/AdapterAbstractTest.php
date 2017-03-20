<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\Test\BaseTest;

use Pseudo\Pdo as PPDO;
use Symfony\Component\EventDispatcher\EventDispatcher;

/**
 * @covers Behance\NBD\Dbal\AdapterAbstract<extended>
 */
class AdapterAbstractTest extends BaseTest {

  private $_table  = 'abc';
  private $_column = 'def';
  private $_result = 10101010101;
  private $_params = [ 678, 91011, 121314 ];
  private $_sql    = "SELECT something FROM somebody";
  private $_where_string = 'abc = 123';

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
    $dispatcher = $this->createMock( EventDispatcher::class, [ 'addListener' ] );
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
   * @dataProvider boolProvider
   */
  public function getColAlias( $master ) {

    $adapter = $this->_buildAdapter( null, [ 'getColumn' ] );
    $adapter->expects( $this->once() )
      ->method( 'getColumn' )
      ->with( $this->_table, $this->_column, $this->_where_string, $master )
      ->will( $this->returnValue( $this->_result ) );

    $this->assertEquals( $this->_result, $adapter->getCol( $this->_table, $this->_column, $this->_where_string, $master ) );

  } // getColAlias


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function fetchColAlias( $master ) {

    $adapter = $this->_buildAdapter( null, [ 'fetchColumn' ] );
    $adapter->expects( $this->once() )
      ->method( 'fetchColumn' )
      ->with( $this->_sql, $this->_params, $master )
      ->will( $this->returnValue( $this->_result ) );

    $this->assertEquals( $this->_result, $adapter->fetchCol( $this->_sql, $this->_params, $master ) );

  } // fetchColAlias


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function query( $master ) {

    $adapter = $this->_buildAdapter( null, [ '_execute' ] );
    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( null, $this->_sql, $this->_params, $master )
      ->will( $this->returnValue( $this->_result ) );

    $this->assertEquals( $this->_result, $adapter->query( $this->_sql, $this->_params, $master ) );

  } // query


  /**
   * @test
   */
  public function queryMaster() {

    $adapter = $this->_buildAdapter( null, [ '_execute' ] );
    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( null, $this->_sql, $this->_params, true )
      ->will( $this->returnValue( $this->_result ) );

    $this->assertEquals( $this->_result, $adapter->queryMaster( $this->_sql, $this->_params ) );

  } // queryMaster


  /**
   * @test
   * @dataProvider boolProvider
   */
  public function queryTable( $master ) {

    $adapter = $this->_buildAdapter( null, [ '_execute' ] );
    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( $this->_table, $this->_sql, $this->_params, $master )
      ->will( $this->returnValue( $this->_result ) );

    $this->assertEquals( $this->_result, $adapter->queryTable( $this->_table, $this->_sql, $this->_params, $master ) );

  } // query


  /**
   * @test
   */
  public function queryTableMaster() {

    $adapter = $this->_buildAdapter( null, [ '_execute' ] );
    $adapter->expects( $this->once() )
      ->method( '_execute' )
      ->with( $this->_table, $this->_sql, $this->_params, true )
      ->will( $this->returnValue( $this->_result ) );

    $this->assertEquals( $this->_result, $adapter->queryTableMaster( $this->_table, $this->_sql, $this->_params ) );

  } // query

  /**
   * @param Behance\NBD\Dbal\ConnectionService $connection
   * @param array                              $functions
   *
   * @return Behance\NBD\Dbal\Adapters\PdoAdapter
   */
  private function _buildAdapter( ConnectionService $connection = null, array $functions = [] ) {

    $connection = $connection ?: $this->_getDisabledMock( ConnectionService::class );

    return $this->_getAbstractMock( AdapterAbstract::class, $functions, [ $connection ] );

  } // _buildAdapter

} // AdapterAbstractTest
