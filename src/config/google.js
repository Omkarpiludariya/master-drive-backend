import dotenv from 'dotenv';
dotenv.config();

import { google } from 'googleapis';

export function createOAuth2Client() {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
  const redirectUri = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:5000/api/accounts/google/callback';

  return new google.auth.OAuth2(clientId, clientSecret, redirectUri);
}

export const GOOGLE_SCOPES = [
  'https://www.googleapis.com/auth/drive.file', // Read/write files created by this app
  'https://www.googleapis.com/auth/userinfo.email',
  'https://www.googleapis.com/auth/userinfo.profile',
];

export const CHUNKS_FOLDER_NAME = 'MasterDrive_Chunks';

export default {
  createOAuth2Client,
  GOOGLE_SCOPES,
  CHUNKS_FOLDER_NAME,
};
