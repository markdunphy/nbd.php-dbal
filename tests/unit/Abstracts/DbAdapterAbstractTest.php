<?php

namespace Behance\Core\Dbal\Adapters;

use Behance\Core\Dbal\Test\BaseTest;
use Symfony\Component\EventDispatcher\EventDispatcher;

class DbAdapterAbstractTest extends BaseTest {

  private $_target = 'Behance\Core\Dbal\Abstracts\DbAdapterAbstract';

  /**
   * @test
   */
  public function construct() {

    $connection = $this->_getDisabledMock( 'Behance\Core\Dbal\Services\ConnectionService' );
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

    $connection = $this->_getDisabledMock( 'Behance\Core\Dbal\Services\ConnectionService', [ 'getOpenedConnections' ] );
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
   * @return Zend_Db_Adapter_Adapter
   */
  private function _createClosableAdapter() {

    $mock = $this->_getDisabledMock( 'Zend\Db\Adapter\Adapter', [ 'closeConnection' ] );

    $mock->expects( $this->once() )
      ->method( 'closeConnection' );

    return $mock;

  } // _createClosableAdapter

} // DbAdapterAbstractTest
