<?php

namespace Behance\NBD\Dbal\Events;

use Behance\NBD\Dbal\DbalException;

use Symfony\Component\EventDispatcher\Event;

class QueryEvent extends Event {

  /**
   * @var \PDOStatement
   */
  private $_statement;


  /**
   * @var string
   */
  private $_query;


  /**
   * @var bool  whether or not the master database is in use
   */
  private $_parameters;


  /**
   * @var bool  whether or not the master database is in use
   */
  private $_use_master;


  /**
   * @var Behance\NBD\Dbal\Exceptions\Exception
   */
  private $_exception;

  /**
   * @var bool
   */
  private $_is_retry;


  /**
   * @param \PDOStatement|string           $statement    either the prepared statement, or the SQL used to prepare
   * @param array                          $parameters   query parameters sent to database
   * @param bool                           $use_master   whether or not master database is in use
   * @param Behance\NBD\Dbal\DbalException $exception
   * @param bool                           $is_retry whether or not it is a retried query
   */
  public function __construct( $statement, array $parameters = null, $use_master = false, DbalException $exception = null, $is_retry = false ) {

    $this->_statement = ( $statement instanceof \PDOStatement )
                        ? $statement
                        : null;

    $this->_query = ( $this->_statement )
                    ? $this->_statement->queryString
                    : $statement;

    $this->_parameters = $parameters;
    $this->_exception  = $exception;
    $this->_use_master = $use_master;
    $this->_is_retry   = $is_retry;

  } // __construct


  /**
   * @return bool
   */
  public function hasStatement() {

    return !empty( $this->_statement );

  } // hasStatement


  /**
   * @return \PDOStatement
   */
  public function getStatement() {

    return $this->_statement;

  } // getStatement


  /**
   * @return bool
   */
  public function hasParameters() {

    return !empty( $this->_parameters );

  } // hasParameters


  /**
   * @return array|null
   */
  public function getParameters() {

    return $this->_parameters;

  } // getParameters


  /**
   * @return string
   */
  public function getQuery() {

    return $this->_query;

  } // getQuery


  /**
   * @return bool
   */
  public function isUsingMaster() {

    return $this->_use_master;

  } // isUsingMaster


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

  /**
   * @return boolean
   */
  public function isRetry() {

    return $this->_is_retry;

  } // isRetry

} // QueryEvent
