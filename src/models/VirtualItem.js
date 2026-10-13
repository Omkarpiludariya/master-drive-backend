import mongoose from 'mongoose';

const chunkManifestSchema = new mongoose.Schema(
  {
    partIndex: {
      type: Number,
      required: true,
    },
    driveAccountId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'DriveAccount',
      required: true,
    },
    googleDriveFileId: {
      type: String,
      required: true,
    },
    size: {
      type: Number,
      required: true,
    },
    md5: {
      type: String,
      default: '',
    },
  },
  { _id: false }
);

const virtualItemSchema = new mongoose.Schema(
  {
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'User',
      required: true,
      index: true,
    },
    name: {
      type: String,
      required: true,
      trim: true,
    },
    type: {
      type: String,
      enum: ['file', 'folder'],
      required: true,
    },
    parentId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'VirtualItem',
      default: null,
      index: true,
    },
    size: {
      type: Number,
      default: 0,
    },
    mimeType: {
      type: String,
      default: function () {
        return this.type === 'folder' ? 'application/vnd.google-apps.folder' : 'application/octet-stream';
      },
    },
    isChunked: {
      type: Boolean,
      default: false,
    },
    totalChunks: {
      type: Number,
      default: 1,
    },
    checksum: {
      type: String,
      default: '',
    },
    thumbnail: {
      type: String,
      default: null,
    },
    isStarred: {
      type: Boolean,
      default: false,
    },
    isTrash: {
      type: Boolean,
      default: false,
      index: true,
    },
    trashedAt: {
      type: Date,
      default: null,
    },
    status: {
      type: String,
      enum: ['uploading', 'ready', 'failed'],
      default: 'ready',
    },
    mediaMetadata: {
      takenAt: {
        type: Date,
        default: null,
        index: true,
      },
      width: {
        type: Number,
        default: null,
      },
      height: {
        type: Number,
        default: null,
      },
      duration: {
        type: Number,
        default: null,
      },
      deviceId: {
        type: String,
        default: null,
        index: true,
      },
      deviceAssetId: {
        type: String,
        default: null,
        index: true,
      },
    },
    chunks: [chunkManifestSchema],
  },
  {
    timestamps: true,
  }
);

// Compound index for quick navigation & search
virtualItemSchema.index({ userId: 1, parentId: 1, isTrash: 1 });
virtualItemSchema.index({ userId: 1, name: 'text' });
virtualItemSchema.index({ userId: 1, isTrash: 1, 'mediaMetadata.takenAt': -1 });
virtualItemSchema.index({ userId: 1, 'mediaMetadata.deviceId': 1, 'mediaMetadata.deviceAssetId': 1 });
virtualItemSchema.index({ userId: 1, checksum: 1 });

export default mongoose.model('VirtualItem', virtualItemSchema);
