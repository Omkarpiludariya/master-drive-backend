import mongoose from 'mongoose';

const driveAccountSchema = new mongoose.Schema(
  {
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'User',
      required: true,
      index: true,
    },
    accountEmail: {
      type: String,
      required: true,
      trim: true,
    },
    displayName: {
      type: String,
      default: '',
    },
    photoUrl: {
      type: String,
      default: '',
    },
    googleUserId: {
      type: String,
      default: '',
    },
    encryptedAccessToken: {
      type: String,
      default: '',
    },
    encryptedRefreshToken: {
      type: String,
      default: '',
    },
    tokenExpiry: {
      type: Date,
      default: null,
    },
    totalQuota: {
      type: Number,
      default: 15 * 1024 * 1024 * 1024, // 15 GB default
    },
    usedQuota: {
      type: Number,
      default: 0,
    },
    folderId: {
      type: String,
      default: '',
    },
    isActive: {
      type: Boolean,
      default: true,
    },
    isMock: {
      type: Boolean,
      default: false,
    },
    lastSyncedAt: {
      type: Date,
      default: Date.now,
    },
  },
  {
    timestamps: true,
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
  }
);

// Virtual for remaining free space
driveAccountSchema.virtual('freeQuota').get(function () {
  return Math.max(0, this.totalQuota - this.usedQuota);
});

// Compound index to prevent duplicate accounts per user
driveAccountSchema.index({ userId: 1, accountEmail: 1 }, { unique: true });

export default mongoose.model('DriveAccount', driveAccountSchema);
