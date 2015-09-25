<?php

namespace Behance\NBD\Dbal;

use Behance\NBD\Dbal\Test\BaseTest;

/**
 * @covers Behance\NBD\Dbal\Sql
 */
class SqlTest extends BaseTest {

  /**
   * @test
   */
  public function toString() {

    $statement = 'NOW()';
    $sql       = new Sql( $statement );

    $this->assertSame( $statement, (string)$sql );

  } // toString

} // SqlTest
