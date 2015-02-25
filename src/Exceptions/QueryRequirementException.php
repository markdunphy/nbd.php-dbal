<?php

namespace Behance\Core\Dbal\Exceptions;

use Behance\Core\Dbal\Exceptions\Exception as BaseException;

/**
 * Thrown when a strict data check is being applied to a query
 */
class QueryRequirementException extends BaseException {}

