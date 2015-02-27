<?php

namespace Behance\NBD\Dbal\Events;

use Behance\NBD\Dbal\Exceptions\Exception as DbException;

use Symfony\Component\EventDispatcher\Event;

use Zend\Db\Adapter\Driver\ResultInterface;
use Zend\Db\Adapter\Driver\StatementInterface;

class QueryEvent extends Event {

  /**
   * @var Zend\Db\Adapter\Driver\StatementInterface
   */
  private $_statement;

  /**
   * @var Zend\Db\Adapter\Driver\ResultInterface
   */
  private $_result;

  /**
   * @var Behance\NBD\Dbal\Exceptions\Exception
   */
  private $_exception;


  /**
   * @param Zend\Db\Adapter\Driver\StatementInterface $statement
   * @param Zend\Db\Adapter\Driver\ResultInterface    $result
   * @param Behance\NBD\Dbal\Exceptions\Exception    $exception
   */
  public function __construct( StatementInterface $statement, ResultInterface $result = null, DbException $exception = null ) {

    $this->_statement = $statement;
    $this->_result    = $result;
    $this->_exception = $exception;

  } // __construct


  /**
   * @return Zend\Db\Adapter\Driver\StatementInterface
   */
  public function getStatement() {

    return $this->_statement;

  } // getStatement


  /**
   * @return bool
   */
  public function hasResult() {

    return !empty( $this->_result );

  } // hasResult


  /**
   * @return
   */
  public function getResult() {

    return $this->_result;

  } // getResult


  /**
   * @return bool
   */
  public function hasException() {

    return !empty( $this->_exception );

  } // hasException


  /**
   * @return Behance\NBD\Dbal\Exceptions\Exception
   */
  public function getException() {

    return $this->_exception;

  } // getException

} // QueryEvent
