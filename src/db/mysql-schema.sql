-- MySQL schema for zenius
-- Critical data is persisted here: folders, files, jobs, providers, system/rclone config.

CREATE TABLE IF NOT EXISTS folders (
  id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  parent_id VARCHAR(64) NULL,
  path TEXT NOT NULL,
  created_at VARCHAR(40) NOT NULL,
  updated_at VARCHAR(40) NOT NULL,
  INDEX idx_folders_parent (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS files (
  id VARCHAR(64) PRIMARY KEY,
  folder_id VARCHAR(64) NOT NULL,
  name VARCHAR(512) NOT NULL,
  original_url TEXT NULL,
  local_path TEXT NULL,
  size BIGINT NOT NULL DEFAULT 0,
  duration DOUBLE NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL,
  progress_download INT NOT NULL DEFAULT 0,
  progress_processing INT NOT NULL DEFAULT 0,
  progress_upload INT NOT NULL DEFAULT 0,
  progress_extra LONGTEXT NULL,
  sync_status INT NOT NULL DEFAULT 0,
  can_delete TINYINT(1) NOT NULL DEFAULT 0,
  created_at VARCHAR(40) NOT NULL,
  updated_at VARCHAR(40) NOT NULL,
  INDEX idx_files_folder (folder_id),
  INDEX idx_files_status (status),
  INDEX idx_files_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS file_providers (
  file_id VARCHAR(64) NOT NULL,
  provider VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  url TEXT NULL,
  remote_file_id TEXT NULL,
  embed_url TEXT NULL,
  error TEXT NULL,
  url_history LONGTEXT NULL,
  updated_at VARCHAR(40) NOT NULL,
  PRIMARY KEY (file_id, provider),
  INDEX idx_file_providers_provider (provider),
  INDEX idx_file_providers_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS jobs (
  id VARCHAR(64) PRIMARY KEY,
  type VARCHAR(32) NOT NULL,
  file_id VARCHAR(64) NULL,
  status VARCHAR(32) NOT NULL,
  progress INT NOT NULL DEFAULT 0,
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 3,
  error TEXT NULL,
  metadata LONGTEXT NULL,
  created_at VARCHAR(40) NOT NULL,
  updated_at VARCHAR(40) NOT NULL,
  started_at VARCHAR(40) NULL,
  completed_at VARCHAR(40) NULL,
  INDEX idx_jobs_status (status),
  INDEX idx_jobs_file (file_id),
  INDEX idx_jobs_type (type),
  INDEX idx_jobs_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS provider_configs (
  provider VARCHAR(64) PRIMARY KEY,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  config LONGTEXT NULL,
  updated_at VARCHAR(40) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS system_state (
  id TINYINT PRIMARY KEY,
  last_check VARCHAR(40) NULL,
  next_scheduled_check VARCHAR(40) NULL,
  primary_provider VARCHAR(64) NOT NULL,
  updated_at VARCHAR(40) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS provider_checks (
  provider VARCHAR(64) PRIMARY KEY,
  payload LONGTEXT NULL,
  checked_at VARCHAR(40) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS rclone_remotes (
  name VARCHAR(128) PRIMARY KEY,
  type VARCHAR(64) NOT NULL,
  parameters LONGTEXT NULL,
  updated_at VARCHAR(40) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS rclone_sync_profiles (
  id VARCHAR(128) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  provider VARCHAR(64) NOT NULL,
  remote_name VARCHAR(128) NOT NULL,
  destination_path TEXT NULL,
  public_base_url TEXT NULL,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  updated_at VARCHAR(40) NOT NULL,
  INDEX idx_rclone_profiles_provider (provider),
  INDEX idx_rclone_profiles_remote (remote_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS rclone_state (
  id TINYINT PRIMARY KEY,
  default_profile_id VARCHAR(128) NULL,
  last_validation LONGTEXT NULL,
  last_validated_at VARCHAR(40) NULL,
  updated_at VARCHAR(40) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
