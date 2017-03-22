<?php

namespace Behance\NBD\Dbal\Test;

use Behance\NBD\Dbal\Factory;

/**
 * When creating integration tests, a table can be quickly created per-test
 * by defining $_tables, where each key in the array defines the table name and the value
 * is the corresponding SQL syntax. Simple and easy.
 */
abstract class IntegrationTest extends BaseTest {

  /**
   * Defines fixtures that are necessary for the current
   *
   * @var string[]  table name => creation syntaxtest class
   */
  protected $_tables;


  /**
   * 1. Determines if test is currently running in integration mode, skips otherwise
   * 2. If necessary creations
   */
  protected function setUp() {

    $config = $this->_getEnvironmentConfig();

    // IMPORTANT: only run integration tests when a database is provided and working
    if ( empty( $config['host'] ) ) {
      $this->markTestSkipped( 'DB not available' );
    }

    if ( $this->_tables ) {

      $adapter = $this->_getLiveAdapter();

      foreach ( $this->_tables as $name => $sql ) {
        $adapter->query( "DROP TABLE IF EXISTS `{$name}`" );
        $adapter->query( $sql );
      }

    } // if tables

    parent::setUp();

  } // setUp

  /**
   * Removes any tables that may have been in use
   */
  protected function tearDown() {

    if ( $this->_tables ) {
      $adapter = $this->_getLiveAdapter();
      $names   = array_keys( $this->_tables );

      foreach ( $names as $name ) {
        $adapter->query( "DROP TABLE `{$name}`" );
      }

    } // if tables

  } // tearDown

  /**
   * @return Behance\NBD\Dbal\AdapterInterface
   */
  protected function _getLiveAdapter() {

    $configs = [
        'master' => $this->_getEnvironmentConfig()
    ];

    return Factory::create( $configs );

  } // _getLiveAdapter

  /**
   * @return Behance\NBD\Dbal\AdapterInterface
   */
  protected function _getLiveReplicatedAdapter() {

    $configs = [
        'master'   => $this->_getEnvironmentConfig(),
        'replicas' => [ $this->_getEnvironmentConfig() ],
    ];

    return Factory::create( $configs );

  } // _getLiveReplicatedAdapter


  /**
   * @return array
   */
  private function _getEnvironmentConfig() {

    return [
        'host'     => getenv( 'CFG_DB_HOST' ),
        'port'     => getenv( 'CFG_DB_PORT' ),
        'username' => getenv( 'CFG_DB_USER' ),
        'password' => getenv( 'CFG_DB_PASS' ),
        'database' => getenv( 'CFG_DB' ),
        'charset'  => getenv( 'CFG_DB_CHARSET' ),
    ];

  } // _getEnvironmentConfig

} // IntegrationTest
