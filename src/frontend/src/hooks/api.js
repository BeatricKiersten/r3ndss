import { useQuery, useMutation, useQueryClient } from 'react-query';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || '';
const isDevelopment = import.meta.env.DEV;

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json'
  },
  timeout: 120000
});

api.interceptors.response.use(null, async (error) => {
  if (error.response?.status === 503) {
    const config = error.config;
    if (!config.__retryCount) config.__retryCount = 0;
    
    if (config.__retryCount < 3) {
      config.__retryCount += 1;
      await new Promise(r => setTimeout(r, 3000 * config.__retryCount));
      return api(config);
    }
  }
  return Promise.reject(error);
});

if (isDevelopment) {
  api.interceptors.request.use((config) => {
    console.log('[API] Request', {
      method: String(config.method || 'get').toUpperCase(),
      url: `${config.baseURL || ''}${config.url || ''}`,
      params: config.params,
      data: config.data,
      headers: config.headers
    });
    return config;
  });

  api.interceptors.response.use(
    (response) => {
      console.log('[API] Response', {
        status: response.status,
        url: `${response.config.baseURL || ''}${response.config.url || ''}`,
        data: response.data,
        headers: response.headers
      });
      return response;
    },
    (error) => {
      console.error('[API] Error', {
        message: error.message,
        method: String(error.config?.method || 'get').toUpperCase(),
        url: `${error.config?.baseURL || ''}${error.config?.url || ''}`,
        params: error.config?.params,
        data: error.config?.data,
        responseStatus: error.response?.status,
        responseData: error.response?.data,
        responseHeaders: error.response?.headers
      });
      return Promise.reject(error);
    }
  );
}

// Folder hooks
export const useFolders = () => {
  return useQuery('folders', async () => {
    const response = await api.get('/api/folders/tree');
    return response.data.data;
  });
};

export const useFolder = (id = 'root') => {
  return useQuery(['folder', id], async () => {
    const response = await api.get(`/api/folders/${id}`);
    return response.data.data;
  }, {
    keepPreviousData: true
  });
};

export const useCreateFolder = () => {
  const queryClient = useQueryClient();
  
  return useMutation(
    async ({ name, parentId }) => {
      const response = await api.post('/api/folders', { name, parentId });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('folders');
      }
    }
  );
};

export const useMoveFolder = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ folderId, newParentId }) => {
      const response = await api.put(`/api/folders/${folderId}/move`, { newParentId });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('folders');
        queryClient.invalidateQueries('files');
      }
    }
  );
};

export const useDeleteFolder = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (folderId) => {
      const response = await api.delete(`/api/folders/${folderId}`);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('folders');
        queryClient.invalidateQueries('files');
      }
    }
  );
};

export const usePurgeFolder = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (folderId) => {
      const response = await api.delete(`/api/folders/${folderId}/purge`);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('folders');
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useDeleteAllFolders = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async () => {
      const response = await api.delete('/api/folders/all');
      return response.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('folders');
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

// File hooks
export const useFiles = (folderId, status) => {
  return useQuery(
    ['files', folderId, status],
    async () => {
      const params = {};
      if (folderId) params.folderId = folderId;
      if (status) params.status = status;
      
      const response = await api.get('/api/files', { params });
      return response.data.data;
    },
    {
      refetchInterval: 3000,
      keepPreviousData: true
    }
  );
};

export const useFile = (id) => {
  return useQuery(
    ['file', id],
    async () => {
      const response = await api.get(`/api/files/${id}`);
      return response.data.data;
    },
    { enabled: !!id }
  );
};

export const useUploadHls = () => {
  return useMutation(async ({ url, folderId, filename, decryptionKey, providers }) => {
    const response = await api.post('/api/upload/hls', {
      url,
      folderId,
      filename,
      decryptionKey,
      providers
    });
    return response.data;
  });
};

export const useUploadFile = () => {
  return useMutation(async ({ file, folderId, filename, providers }) => {
    const formData = new FormData();
    formData.append('file', file);

    if (folderId) formData.append('folderId', folderId);
    if (filename) formData.append('filename', filename);
    if (providers?.length) formData.append('providers', JSON.stringify(providers));

    const response = await api.post('/api/upload/file', formData, {
      headers: {
        'Content-Type': 'multipart/form-data'
      }
    });

    return response.data;
  });
};

export const useZeniusInstanceDetails = () => {
  return useMutation(async ({
    urlShortId,
    headersRaw,
    refererPath
  }) => {
    const response = await api.post('/api/zenius/details', {
      urlShortId,
      headersRaw,
      refererPath
    });

    return response.data.data;
  });
};

export const useZeniusDownload = () => {
  return useMutation(async ({
    urlShortId,
    headersRaw,
    refererPath,
    folderId,
    filename,
    providers
  }) => {
    const response = await api.post('/api/zenius/download', {
      urlShortId,
      headersRaw,
      refererPath,
      folderId,
      filename,
      providers
    });

    return response.data;
  });
};

export const useZeniusBatchChain = () => {
  return useMutation(async ({
    rootCgId,
    targetCgSelector,
    parentContainerName,
    headersRaw,
    refererPath,
    sessionId,
    containerOffset,
    containerLimit,
    timeBudgetMs
  }) => {
    const response = await api.post('/api/zenius/batch-chain', {
      rootCgId,
      targetCgSelector,
      parentContainerName,
      headersRaw,
      refererPath,
      sessionId,
      containerOffset,
      containerLimit,
      timeBudgetMs
    }, {
      timeout: 90000
    });

    return response.data.data;
  });
};

export const useZeniusBatchDownload = () => {
  return useMutation(async ({
    rootCgId,
    targetCgSelector,
    parentContainerName,
    headersRaw,
    refererPath,
    folderId,
    providers,
    sessionId,
    containerOffset,
    containerLimit,
    timeBudgetMs
  }) => {
    const response = await api.post('/api/zenius/batch-download', {
      rootCgId,
      targetCgSelector,
      parentContainerName,
      headersRaw,
      refererPath,
      folderId,
      providers,
      sessionId,
      containerOffset,
      containerLimit,
      timeBudgetMs
    }, {
      timeout: 90000
    });

    return response.data;
  });
};

export const useZeniusCancelAll = () => {
  return useMutation(async () => {
    const response = await api.post('/api/zenius/cancel-all');
    return response.data;
  });
};

export const useZeniusResetFiles = () => {
  const queryClient = useQueryClient();
  
  return useMutation(
    async () => {
      const response = await api.post('/api/zenius/reset-files');
      return response.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('jobs');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useZeniusQueueStatus = (options = {}) => {
  const {
    enabled = true,
    refetchInterval = 15000,
    refetchIntervalInBackground = true
  } = options;

  return useQuery(
    ['zenius-queue-status'],
    async () => {
      const response = await api.get('/api/zenius/queue-status');
      return response.data.data;
    },
    {
      enabled,
      refetchInterval,
      refetchIntervalInBackground,
      staleTime: 3000
    }
  );
};

export const useSetMaxConcurrent = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (maxConcurrent) => {
      const response = await api.put('/api/zenius/max-concurrent', { maxConcurrent });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('zenius-queue-status');
      }
    }
  );
};

export const useUploadConcurrency = () => {
  return useQuery(
    ['upload-concurrency'],
    async () => {
      const response = await api.get('/api/zenius/upload-concurrency');
      return response.data.data;
    },
    { staleTime: 10000 }
  );
};

export const useSetUploadConcurrency = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (updates) => {
      const response = await api.put('/api/zenius/upload-concurrency', updates);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('upload-concurrency');
      }
    }
  );
};

export const useWebhookConfig = () => {
  return useQuery(
    ['zenius-webhook-config'],
    async () => {
      const response = await api.get('/api/zenius/webhook');
      return response.data.data;
    },
    { staleTime: 30000 }
  );
};

export const useUpdateWebhookConfig = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (updates) => {
      const response = await api.put('/api/zenius/webhook', updates);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('zenius-webhook-config');
      }
    }
  );
};

export const useTestWebhook = () => {
  return useMutation(
    async () => {
      const response = await api.post('/api/zenius/webhook/test');
      return response.data.data;
    }
  );
};

export const useRetryUpload = () => {
  const queryClient = useQueryClient();
  
  return useMutation(
    async (fileId) => {
      const response = await api.post(`/api/files/${fileId}/retry`);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
      }
    }
  );
};

export const useDeleteFile = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (fileId) => {
      const response = await api.delete(`/api/files/${fileId}`);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
      }
    }
  );
};

export const useForceDeleteFile = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (fileId) => {
      const response = await api.post(`/api/files/${fileId}/delete-force`);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useMoveFile = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ fileId, folderId }) => {
      const response = await api.put(`/api/files/${fileId}/move`, { folderId });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('folders');
      }
    }
  );
};

export const useFailedFiles = () => {
  return useQuery(
    ['files', null, 'failed'],
    async () => {
      const response = await api.get('/api/files', { params: { status: 'failed' } });
      return response.data.data;
    },
    {
      refetchInterval: false,
      enabled: false
    }
  );
};

export const useDeleteAllFailedFiles = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async () => {
      const response = await api.post('/api/files/bulk/delete-failed');
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

// Dashboard hooks
export const useDashboard = () => {
  return useQuery(
    'dashboard',
    async () => {
      const response = await api.get('/api/dashboard');
      return response.data.data;
    },
    { refetchInterval: 2000 }
  );
};

export const useStats = () => {
  return useQuery(
    'stats',
    async () => {
      const response = await api.get('/api/stats');
      return response.data.data;
    },
    { refetchInterval: 5000 }
  );
};

export const useProcesses = () => {
  return useQuery(
    'processes',
    async () => {
      const response = await api.get('/api/processes');
      return response.data.data;
    },
    { refetchInterval: 1000 }
  );
};

export const useProviders = () => {
  return useQuery(
    'providers',
    async () => {
      const response = await api.get('/api/providers');
      return response.data.data;
    },
    { refetchInterval: 5000 }
  );
};

export const useToggleProvider = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ name, enabled }) => {
      const response = await api.put(`/api/providers/${encodeURIComponent(name)}`, { enabled });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('providers');
      }
    }
  );
};

export const useProvidersStatus = () => {
  return useQuery(
    'providers-status',
    async () => {
      const response = await api.get('/api/providers/status');
      return response.data.data;
    },
    { refetchInterval: 30000 }
  );
};

export const useProviderStatus = (provider) => {
  return useQuery(
    ['provider-status', provider],
    async () => {
      const response = await api.get(`/api/providers/${encodeURIComponent(provider)}/status`);
      return response.data.data;
    },
    { enabled: !!provider, refetchInterval: 30000 }
  );
};

export const useCheckProvider = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ provider, autoReuploadMissing = false }) => {
      const response = await api.post(`/api/providers/${encodeURIComponent(provider)}/check`, { autoReuploadMissing });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('providers-status');
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
        queryClient.invalidateQueries('last-check');
      }
    }
  );
};

export const useCheckSelectedProviders = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ providers, autoReuploadMissing = false }) => {
      const response = await api.post('/api/providers/check', { providers, autoReuploadMissing });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('providers-status');
        queryClient.invalidateQueries('provider-check-snapshots');
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
        queryClient.invalidateQueries('last-check');
      }
    }
  );
};

export const useClearMissingProviderLinks = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ provider, reason }) => {
      const response = await api.post(`/api/providers/${encodeURIComponent(provider)}/clear-missing-links`, { reason });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
        queryClient.invalidateQueries('providers-status');
        queryClient.invalidateQueries('provider-check-snapshots');
      }
    }
  );
};

export const useProviderCheckSnapshots = () => {
  return useQuery(
    'provider-check-snapshots',
    async () => {
      const response = await api.get('/api/providers/check-snapshots');
      return response.data.data;
    },
    { refetchInterval: 30000 }
  );
};

export const useFileProvidersStatus = (fileId) => {
  return useQuery(
    ['file-providers-status', fileId],
    async () => {
      const response = await api.get(`/api/files/${fileId}/providers/status`);
      return response.data.data;
    },
    { enabled: !!fileId, refetchInterval: 60000 }
  );
};

export const useReuploadToProvider = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ fileId, provider, source }) => {
      const response = await api.post(`/api/files/${fileId}/reupload/${encodeURIComponent(provider)}`, { source });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useCopyToProvider = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ fileId, targetProvider }) => {
      const response = await api.post(`/api/files/${fileId}/copy/${encodeURIComponent(targetProvider)}`);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useClearFileProviderLink = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ fileId, provider, reason }) => {
      const response = await api.delete(`/api/files/${fileId}/providers/${encodeURIComponent(provider)}`, {
        data: { reason }
      });
      return response.data.data;
    },
    {
      onSuccess: (_, variables) => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
        queryClient.invalidateQueries(['file', variables?.fileId]);
        queryClient.invalidateQueries(['file-providers-status', variables?.fileId]);
      }
    }
  );
};

export const useQueueTransferJob = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ sourceUrl, targetProvider = 'seekstreaming', folderId = 'root', filename = null }) => {
      const response = await api.post('/api/jobs/transfer', {
        sourceUrl,
        targetProvider,
        folderId,
        filename
      });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useJobs = (filters = {}) => {
  return useQuery(
    ['jobs', filters],
    async () => {
      const params = {};
      if (filters.status) params.status = filters.status;
      if (filters.type) params.type = filters.type;
      if (filters.fileId) params.fileId = filters.fileId;
      if (filters.limit) params.limit = filters.limit;

      const response = await api.get('/api/jobs', { params });
      return response.data.data;
    },
    { refetchInterval: 2000 }
  );
};

export const useCancelJob = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (jobId) => {
      const response = await api.post(`/api/jobs/${jobId}/cancel`);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('jobs');
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useDeleteJob = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (jobId) => {
      const response = await api.delete(`/api/jobs/${jobId}`);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('jobs');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useCancelAllJobs = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async () => {
      const response = await api.post('/api/jobs/cancel-all');
      return response.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('jobs');
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useClearJobLogs = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async () => {
      const response = await api.post('/api/jobs/clear-logs');
      return response.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('jobs');
        queryClient.invalidateQueries('dashboard');
      }
    }
  );
};

export const useSystemCheck = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async ({ providers, autoReuploadMissing = false } = {}) => {
      const response = await api.post('/api/system/check', { providers, autoReuploadMissing });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('dashboard');
        queryClient.invalidateQueries('files');
        queryClient.invalidateQueries('providers-status');
        queryClient.invalidateQueries('last-check');
      }
    }
  );
};

export const useLastCheck = () => {
  return useQuery(
    'last-check',
    async () => {
      const response = await api.get('/api/system/last-check');
      return response.data.data;
    },
    { refetchInterval: 60000 }
  );
};

export const usePrimaryProvider = () => {
  return useQuery(
    'primary-provider',
    async () => {
      const response = await api.get('/api/system/primary-provider');
      return response.data.data;
    },
    { refetchInterval: 10000 }
  );
};

export const useUpdatePrimaryProvider = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (provider) => {
      const response = await api.put('/api/system/primary-provider', { provider });
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('primary-provider');
        queryClient.invalidateQueries('providers');
      }
    }
  );
};

export const useRcloneConfig = () => {
  return useQuery(
    'rclone-config',
    async () => {
      const response = await api.get('/api/system/rclone-config');
      return response.data.data;
    },
    { refetchInterval: 15000 }
  );
};

export const useUpdateRcloneConfig = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async (payload) => {
      const response = await api.put('/api/system/rclone-config', payload);
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('rclone-config');
        queryClient.invalidateQueries('providers-status');
      }
    }
  );
};

export const useValidateRclone = () => {
  const queryClient = useQueryClient();

  return useMutation(
    async () => {
      const response = await api.post('/api/system/rclone-config/validate');
      return response.data.data;
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries('rclone-config');
        queryClient.invalidateQueries('providers-status');
      }
    }
  );
};
