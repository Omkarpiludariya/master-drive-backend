export function errorHandler(err, req, res, next) {
  console.error('[Error Handler]', err);

  let status = err.status || err.statusCode || 500;
  let message = err.message || 'Internal server error';

  // Handle Mongoose CastError (e.g. invalid ObjectId)
  if (err.name === 'CastError') {
    status = 400;
    message = `Invalid ID format for field '${err.path}'`;
  }

  // Handle Mongoose ValidationError
  if (err.name === 'ValidationError') {
    status = 400;
  }

  res.status(status).json({
    error: message,
    code: err.code || undefined,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined,
  });
}

export default errorHandler;
