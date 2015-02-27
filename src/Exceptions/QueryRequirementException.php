<?php

namespace Behance\NBD\Dbal\Exceptions;

use Behance\NBD\Dbal\Exceptions\Exception as BaseException;

/**
 * Thrown when a strict data check is being applied to a query
 */
class QueryRequirementException extends BaseException {}
