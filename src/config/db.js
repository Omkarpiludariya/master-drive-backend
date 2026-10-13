import mongoose from 'mongoose';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.resolve(__dirname, '../../.mongo_data');

import dns from 'dns';

// Ensure cloud SRV resolution works across ISPs and Windows environments
try {
  dns.setServers(['8.8.8.8', '1.1.1.1']);
} catch (_) {}

let memoryServer = null;

export async function connectDB() {
  const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017/master_drive';

  try {
    // Attempt connection (allow 8s for cloud Atlas TLS handshake)
    await mongoose.connect(uri, {
      serverSelectionTimeoutMS: 8000,
    });
    console.log(`[Database] Connected to MongoDB at ${uri}`);
  } catch (err) {
    console.warn(`[Database] Primary MongoDB not available at ${uri}: ${err.message}`);

    if (process.env.NODE_ENV !== 'production') {
      const isTest = process.env.NODE_ENV === 'test';
      console.log(`[Database] Initializing ${isTest ? 'isolated test' : 'persistent local'} MongoDB storage...`);
      try {
        const { MongoMemoryServer } = await import('mongodb-memory-server');

        let serverOptions = {};
        if (!isTest) {
          if (!fs.existsSync(DATA_DIR)) {
            fs.mkdirSync(DATA_DIR, { recursive: true });
          }
          serverOptions = {
            instance: {
              dbPath: DATA_DIR,
              storageEngine: 'wiredTiger',
            },
          };
        }

        memoryServer = await MongoMemoryServer.create(serverOptions);
        const memUri = memoryServer.getUri();
        await mongoose.connect(memUri);
        console.log(`[Database] Connected to ${isTest ? 'test in-memory' : 'persistent local (.mongo_data)'} database`);
      } catch (memErr) {
        console.error('[Database] Failed to start MongoDB server:', memErr);
        throw memErr;
      }
    } else {
      throw err;
    }
  }

  mongoose.connection.on('error', (err) => {
    console.error('[Database] MongoDB connection error:', err);
  });

  mongoose.connection.on('disconnected', () => {
    console.warn('[Database] MongoDB disconnected');
  });
}

export async function disconnectDB() {
  await mongoose.disconnect();
  if (memoryServer) {
    await memoryServer.stop();
  }
}

export default connectDB;
