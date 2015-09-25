<?php

namespace Behance\NBD\Dbal;

/**
 * A container for raw SQL parameters or statements that should/cannot be escaped, for example: functions, incrementers/decrementers, time operations, etc.
 */
class Sql {

  private $_value;

  public function __construct( $value ) {

    $this->_value = $value;

  } // __construct

  public function __toString() {

    return $this->_value;

  } // __toString

} // Sql
