<?php

namespace Behance\Core\Dbal\Adapters\Connection;

use Behance\Core\Dbal\Interfaces\ConnectionAdapterInterface;

use Zend\Db\Adapter\Adapter as DbAdapter;

class ZendConnectionAdapter implements ConnectionAdapterInterface {

  /**
   * @param array $config
   *
   * @return Zend\Db\Adapter\Adapter
   */
  public function build( array $config ) {

    return new DbAdapter( $config );

  } // build

} // ZendConnectionAdapter
