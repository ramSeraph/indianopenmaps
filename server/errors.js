class HttpError extends Error {
  constructor(message, statusCode = 500) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    Error.captureStackTrace(this, this.constructor);
  }
}

class NotFoundError extends HttpError {
  constructor(message = 'Resource not found') {
    super(message, 404);
  }
}

class BadRequestError extends HttpError {
  constructor(message = 'Bad request') {
    super(message, 400);
  }
}

class ForbiddenError extends HttpError {
  constructor(message = 'Forbidden') {
    super(message, 403);
  }
}

class UnauthorizedError extends HttpError {
  constructor(message = 'Unauthorized') {
    super(message, 401);
  }
}

class UnknownError extends HttpError {
  constructor(message = 'An unknown error occurred') {
    super(message, 500);
  }
}

class ResourceUnavailableError extends HttpError {
  constructor(message = 'Resource unavailable') {
    super(message, 424); // 424 Failed Dependency
  }
}

module.exports = {
  HttpError,
  NotFoundError,
  BadRequestError,
  ForbiddenError,
  UnauthorizedError,
  UnknownError,
  ResourceUnavailableError
};
