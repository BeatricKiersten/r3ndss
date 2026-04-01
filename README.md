# HLS-to-MP4 Multi-Storage Backup & Streaming Platform

A production-ready platform for downloading encrypted HLS streams, converting them to MP4, and backing them up to multiple cloud storage providers in parallel.

## Features

- **HLS Processing**: Download and decrypt HLS streams using FFmpeg
- **Multi-Provider Backup**: Upload to 4 storage providers simultaneously
  - Rclone Storage (configurable remote)
  - Voe.sx
  - Catbox.moe
  - SeekStreaming
- **Fault Tolerance**: Automatic retries, timeout handling, and stream uploading
- **MySQL Core Storage**: Critical data persists in MySQL (files/jobs/folders/providers/system)
- **Real-time Dashboard**: WebSocket-based live updates for job progress
- **Virtual Folder System**: Organize files in hierarchical folders
- **Smart Cleanup**: Local files only deleted after 100% provider sync confirmation

## Architecture

### Backend Components

```
src/
├── server.js              # Express server & WebSocket setup
├── db/
│   ├── handler.js         # MySQL database handler
│   └── mysql-schema.sql   # MySQL schema reference
├── services/
│   ├── VideoProcessor.js  # FFmpeg HLS processing
│   └── UploaderService.js # Parallel upload queue/workers
└── providers/
    ├── rclone.js          # Rclone-backed storage adapter
    ├── voesx.js           # Voe.sx adapter
    ├── catbox.js          # Catbox adapter
    └── seekstreaming.js   # SeekStreaming adapter
```

### Frontend Components

```
src/frontend/src/
├── App.js
├── store/
│   └── websocketStore.js  # Zustand state management
├── hooks/
│   └── api.js             # React Query hooks
└── pages/
    ├── UploadPage.js      # HLS URL submission
    ├── FileListPage.js    # Virtual folder browser
    ├── VideoPlayerPage.js # MP4 player with source switching
    └── BackupStatusPage.js # Real-time dashboard
```

## Quick Start

### Prerequisites

- Node.js 18+
- FFmpeg installed on system
- MySQL 8+ (or MariaDB compatible)
- Redis (optional, for production queue)

### Installation

```bash
# Install backend dependencies
npm install

# Install frontend dependencies
cd src/frontend
npm install
cd ../..

# Copy environment file
cp .env.example .env

# Edit .env with MySQL credentials + provider API keys
```

### Running

```bash
# Development mode (concurrently)
npm run dev

# Or separately
npm start          # Backend
npm run frontend   # Frontend
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/upload/hls | Submit HLS URL for processing |
| GET | /api/folders | List folder tree |
| GET | /api/folders/:id | Get folder contents |
| GET | /api/files | List files |
| GET | /api/files/:id | Get file details |
| GET | /api/files/:id/status | Get upload status |
| POST | /api/files/:id/retry | Retry failed uploads |
| DELETE | /api/files/:id | Delete file (100% sync only) |
| GET | /api/dashboard | Get dashboard data |
| GET | /api/stats | Get system stats |

## Database Schema

Database utama menggunakan **MySQL**. Tabel inti:

- `folders`
- `files`
- `file_providers`
- `jobs`
- `provider_configs`
- `provider_checks`
- `system_state`
- `rclone_remotes`
- `rclone_sync_profiles`
- `rclone_state`

Referensi SQL lengkap tersedia di `src/db/mysql-schema.sql`.

### Legacy JSON Migration (optional)

Jika masih punya data lama di `data/db.json`, jalankan:

```bash
npm run db:migrate:json
```

Atau tentukan path file JSON secara manual:

```bash
node src/scripts/migrate-json-to-mysql.js ./path/to/db.json
```

## Key Features Explained

### 1. MySQL Database Layer (`src/db/handler.js`)

- Semua data penting dipindah ke MySQL.
- Inisialisasi schema dilakukan otomatis saat startup.
- Tidak menyimpan log verbose ke database agar tetap ringan.

### 2. Video Processor (`src/services/VideoProcessor.js`)

- Spawns FFmpeg as child process
- Parses stderr for progress updates
- Handles decryption keys for encrypted HLS
- Implements retry logic with exponential backoff
- 2-hour timeout per job

### 3. Uploader Service (`src/services/UploaderService.js`)

- Uses `p-limit` for concurrency control
- Parallel uploads to all 4 providers
- Stream-based uploading to prevent memory exhaustion
- AbortController for cancellation support
- Automatic retry with exponential backoff

### 4. Provider Adapters

Each provider implements a common interface:
- `upload(filePath, fileName, onProgress, signal)` → `{ url, fileId }`
- `delete(fileId)` → `{ deleted: true }`

### 5. Frontend State Management

Uses **Zustand** for state management with three main stores:
- `useWebSocketStore`: WebSocket connection & real-time events
- `useFileStore`: File and folder data
- `usePlayerStore`: Video player state with source switching

## Configuration

### Provider API Setup

**Rclone Storage:**
1. Konfigurasi credential remote di halaman `/rclone`
2. Tambahkan `remotes` dan `sync profiles`
3. Set profile default untuk provider `rclone`

**Voe.sx:**
1. Register at voe.sx
2. Get API key from account settings
3. Set `VOE_API_KEY`

**Catbox:**
1. Optional: Register for user hash
2. Set `CATBOX_USER_HASH` (optional)

**SeekStreaming:**
1. Register at seekstreaming.com
2. Generate API key
3. Set `SEEK_API_KEY`

## Security Considerations

- Input validation on all API endpoints
- File path sanitization
- CORS configuration
- No sensitive data in logs
- Lightweight logging by default (`DEBUG_HTTP_VERBOSE=false`)
- Environment variables for secrets

## Troubleshooting

### FFmpeg not found
```bash
# Ubuntu/Debian
sudo apt-get install ffmpeg

# macOS
brew install ffmpeg
```

### MySQL connection error
Pastikan konfigurasi benar. Bisa pakai field terpisah (`MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`) atau satu URL `MYSQL_URL` (contoh dengan `ssl-mode=REQUIRED`).
Jika provider database memakai certificate chain non-standar, set `MYSQL_SSL_REJECT_UNAUTHORIZED=false`.

### Upload failures
Check provider-specific logs and verify API credentials.

## License

MIT
