<?php

namespace Behance\NBD\Dbal\Test;

abstract class BaseTest extends \PHPUnit_Framework_TestCase {

  /**
   * @param string $class
   * @param array  $functions
   *
   * @return mixed  instace mock of $class
   */
  protected function _getDisabledMock( $class, array $functions = null ) {

    return $this->getMockBuilder( $class )
      ->setMethods( $functions )
      ->disableOriginalConstructor()
      ->getMock();

  } // _getDisabledMock


  /**
   * @param string $class
   * @param array  $functions
   * @param array  $arguments
   *
   * @return mixed  instace mock of $class
   */
  protected function _getAbstractMock( $class, array $functions = null, array $arguments = [] ) {

    return $this->getMockForAbstractClass( $class, $arguments, '', true, true, true, $functions );

  } // _getAbstractMock

} // BaseTest
