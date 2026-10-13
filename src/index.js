import dotenv from 'dotenv';
dotenv.config();

import express from 'express';
import cors from 'cors';
import { connectDB } from './config/db.js';
import authRoutes from './routes/authRoutes.js';
import accountRoutes from './routes/accountRoutes.js';
import fileRoutes from './routes/fileRoutes.js';
import { errorHandler } from './middleware/errorHandler.js';

const app = express();
const PORT = process.env.PORT || 5000;
const CLIENT_URL = process.env.CLIENT_URL || 'http://localhost:3001';

// CORS configuration (allow Web and Mobile Native clients)
app.use(
  cors({
    origin: true,
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Range'],
    exposedHeaders: ['Content-Range', 'Accept-Ranges', 'Content-Length', 'Content-Disposition'],
  })
);

// Body parsers (exclude raw multipart streams which busboy parses in fileController)
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'Master Google Drive API',
    timestamp: new Date().toISOString(),
  });
});

// Mount Routes
app.use('/api/auth', authRoutes);
app.use('/api/accounts', accountRoutes);
app.use('/api/files', fileRoutes);

// Error Handling Middleware
app.use(errorHandler);

// Connect DB and Start Server
export async function startServer() {
  try {
    await connectDB();
    const server = app.listen(PORT, () => {
      console.log(`[Master Drive Server] Running on http://localhost:${PORT}`);
      console.log(`[Master Drive Server] Client configured at ${CLIENT_URL}`);
    });
    // Ensure large file uploads and downloads do not get timed out by Node.js
    server.timeout = 0;
    server.requestTimeout = 0;
    server.keepAliveTimeout = 65000;
    server.headersTimeout = 66000;
    return server;
  } catch (err) {
    console.error('[Master Drive Server] Failed to start server:', err);
    process.exit(1);
  }
}

if (process.env.NODE_ENV !== 'test') {
  startServer();
}

export default app;
