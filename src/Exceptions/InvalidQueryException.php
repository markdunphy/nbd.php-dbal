<?php

namespace Behance\NBD\Dbal\Exceptions;

use Behance\NBD\Dbal\Exceptions\Exception as BaseException;

/**
 * Thrown when query has a typo or statement-related error, typically based on developer error
 */
class InvalidQueryException extends BaseException {}
