<?php

namespace Behance\Core\Dbal\Interfaces;

interface ConnectionFactoryInterface {

  /**
   * @param array $config
   *
   * @return mixed  DB connection based on adapter type
   */
  public function build( array $config );

} // ConnectionFactoryInterface
