export class HttpError extends Error {
  constructor(message, statusCode = 500) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class NotFoundError extends HttpError {
  constructor(message = 'Resource not found') {
    super(message, 404);
  }
}

export class BadRequestError extends HttpError {
  constructor(message = 'Bad request') {
    super(message, 400);
  }
}

export class ForbiddenError extends HttpError {
  constructor(message = 'Forbidden') {
    super(message, 403);
  }
}

export class UnauthorizedError extends HttpError {
  constructor(message = 'Unauthorized') {
    super(message, 401);
  }
}

export class UnknownError extends HttpError {
  constructor(message = 'An unknown error occurred') {
    super(message, 500);
  }
}

export class ResourceUnavailableError extends HttpError {
  constructor(message = 'Resource unavailable') {
    super(message, 424); // 424 Failed Dependency
  }
}

export class UpstreamHttpError extends HttpError {
  constructor(response) {
    const upstreamStatus = response.status;
    // Upstream archive failures become gateway errors for Worker clients.
    let statusCode = 502;
    if (upstreamStatus === 429 || upstreamStatus === 503) statusCode = 503;
    if (upstreamStatus === 408 || upstreamStatus === 504) statusCode = 504;
    super(`Bad response code: ${upstreamStatus}`, statusCode);
    this.upstreamStatus = upstreamStatus;
    this.retryAfter = response.headers.get('Retry-After');
  }
}
