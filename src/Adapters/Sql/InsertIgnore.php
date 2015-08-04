<?php

namespace Behance\NBD\Dbal\Adapters\Sql;

use Zend\Db\Sql\Insert;

/**
 * Extends Zend DB's insert SQL class to do nothing but alter the base statement
 */
class InsertIgnore extends Insert {

    /**
     * @var array Specification array
     */
    protected $specifications = [
        self::SPECIFICATION_INSERT => 'INSERT IGNORE INTO %1$s (%2$s) VALUES (%3$s)',
        self::SPECIFICATION_SELECT => 'INSERT IGNORE INTO %1$s %2$s %3$s',
    ];

} // InsertIgnore
