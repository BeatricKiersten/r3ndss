# Project Codebase Information

This markdown file provides an overview of the Zenius batch downloader and video processor codebase. **This is pure context for agents.** Do not spend time over-analyzing non-essential parts. Focus on the file structure to navigate efficiently.

## Core Purpose
The project is an Express-based Node.js backend with a React frontend that handles batch downloading, processing (FFmpeg), and uploading of video contents from an external educational platform (Zenius). It supports container/tree discovery, concurrent downloading, database tracking (MySQL), provider abstraction for external uploads (Rclone, Voesx, Catbox, Seekstreaming), and webhook notifications.

## Project Structure

### `/src` - Backend Core

#### Entry & Configuration
*   **`server.js`**: Main entry point for the Express application. Sets up middlewares, WebSocket, and mounts routes.
*   **`config/index.js`**: Central configuration loader (environment variables, defaults for FFmpeg, DB, uploads).
*   **`services/runtime.js`**: Initializes and exports singleton instances of core services (`db`, `videoProcessor`, `uploaderService`, `eventEmitter`).

#### Controllers (`/src/controllers/`)
*   **`zeniusController.js`**: **(Massive)** Handles the entire Zenius integration logic. Includes tree traversal (`buildBatchChain`), FFmpeg queueing (`DownloadQueue`), background batch loop (`processBackgroundBatchRun`), cancellation, and reset logic.
*   **`fileController.js`, `folderController.js`**: Manages CRUD for local files and virtual folders in the DB.
*   **`jobController.js`**: Manages processing/upload job statuses.
*   **`providerController.js`**: Lists and manages external storage providers.

#### Services (`/src/services/`)
*   **`VideoProcessor.js`**: Manages FFmpeg child processes to download HLS streams. Includes progress parsing, timeout handling, and smart error parsing (ignores transient EOF warnings).
*   **`UploaderService.js`**: Manages the queue for uploading processed MP4 files to configured external providers. Handles retries and fallback if an upload fails.
*   **`webhookService.js`**: Handles external webhook notifications for batch completions and item-level errors.
*   **`providerRegistry.js`**: Loads and provides access to the different upload providers.

#### Providers (`/src/providers/`)
*   **`rclone.js`**: Generic wrapper for Rclone-backed storage (S3, GDrive, B2).
*   **`seekstreaming.js`, `voesx.js`, `catbox.js`**: Specific implementation wrappers for various video/file hosting providers.

#### Database (`/src/db/`)
*   **`handler.js`**: Abstraction layer for MySQL queries. Contains all SQL for files, folders, jobs, batch sessions, and config.
*   **`mysql-schema.sql`**: The database schema definition.

#### Routes (`/src/routes/`)
*   **`zenius.js`**: Routes for Zenius operations (`/details`, `/download`, `/batch-chain`, `/batch-download`, etc.).
*   Other files route to their respective controllers (`files.js`, `folders.js`, `jobs.js`, etc.).

### `/src/frontend` - React Frontend
*   **`src/App.jsx`**: Main React component, routing, and layout shell.
*   **`src/pages/ZeniusPage.jsx`**: **(Massive)** The primary UI for interacting with the Zenius downloader. Contains tabs for single download and batch downloading, status monitoring, and folder selection.
*   **`src/hooks/`**: Custom React Query hooks (e.g., `api.js` for calling backend endpoints).

## Key Workflows to Note
1.  **Batch Downloading (`zeniusController.js`)**:
    *   Finds videos recursively from a root CGroup ID.
    *   Discovers all nested containers and queues their `video-url`.
    *   Handles duplicate detection based on file names and db statuses (`completed`, `processing`, `failed`).
    *   Executes in a background loop via `processBackgroundBatchRun`.
2.  **Video Processing (`VideoProcessor.js`)**:
    *   Uses `spawn` to run `ffmpeg` for HLS downloading.
    *   `_parseFfmpegError` is optimized to ignore transient `End of file` reconnect warnings.
3.  **Uploading (`UploaderService.js`)**:
    *   Picks up where `VideoProcessor` leaves off (MP4 file ready) and queues uploads to providers.