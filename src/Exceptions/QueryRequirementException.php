<?php

namespace Behance\NBD\Dbal\Exceptions;

use Behance\NBD\Dbal\DbalException;

/**
 * Thrown when a required element is missing for a particular query (ex. missing where from update)
 * Prevents invalid/dangerous query from reaching database
 */
class QueryRequirementException extends DbalException {}
