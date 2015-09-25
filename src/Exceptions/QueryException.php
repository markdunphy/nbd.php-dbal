<?php

namespace Behance\NBD\Dbal\Exceptions;

use Behance\NBD\Dbal\DbalException;

/**
 * Thrown when a query is unable to be completed, marked as failed by the database itself
 */
class QueryException extends DbalException {}
