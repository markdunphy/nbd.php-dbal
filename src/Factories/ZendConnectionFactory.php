<?php

namespace Behance\NBD\Dbal\Factories;

use Behance\NBD\Dbal\Interfaces\ConnectionFactoryInterface;

use Zend\Db\Adapter\Adapter as DbAdapter;

class ZendConnectionFactory implements ConnectionFactoryInterface {

  /**
   * @param array $config
   *
   * @return Zend\Db\Adapter\Adapter
   */
  public function build( array $config ) {

    return new DbAdapter( $config );

  } // build

} // ZendConnectionFactory
