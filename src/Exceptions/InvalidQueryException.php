<?php

namespace Behance\NBD\Dbal\Exceptions;

use Behance\NBD\Dbal\DbalException;

/**
 * Thrown when query has a typo or statement-related issue, typically based on developer error
 * Prevents query from reaching database
 */
class InvalidQueryException extends DbalException {}
