/**
 * API client configuration and functions
 */

import axios from 'axios';

// Create axios instance with base configuration
export const api = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 30000, // 30 seconds
});

// Request interceptor for logging
api.interceptors.request.use(
  (config) => {
    console.log(`Making ${config.method?.toUpperCase()} request to ${config.url}`);
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error('API Error:', error.response?.data || error.message);
    return Promise.reject(error);
  }
);

// API functions
export const uploadCSV = async (file: File, sampleSize?: number) => {
  const formData = new FormData();
  formData.append('file', file);
  
  const params = new URLSearchParams();
  if (sampleSize) {
    params.append('sample_size', sampleSize.toString());
  }
  
  const response = await api.post(`/api/v1/upload?${params}`, formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  });
  
  return response.data;
};

export const getProcessingStatus = async (datasetId: string) => {
  const response = await api.get(`/api/v1/upload/${datasetId}/status`);
  return response.data;
};

export const getDashboard = async (datasetId: string) => {
  const response = await api.get(`/api/v1/dashboard/${datasetId}`);
  return response.data;
};

export const getDashboardPreview = async (datasetId: string, limit = 100) => {
  const response = await api.get(`/api/v1/dashboard/${datasetId}/preview?limit=${limit}`);
  return response.data;
};

export const getDataProfile = async (datasetId: string) => {
  const response = await api.get(`/api/v1/analytics/${datasetId}/profile`);
  return response.data;
};

export const getDataSample = async (datasetId: string, limit = 100) => {
  const response = await api.get(`/api/v1/analytics/${datasetId}/sample?limit=${limit}`);
  return response.data;
};

export const executeQuery = async (datasetId: string, sql: string) => {
  const response = await api.post(`/api/v1/analytics/${datasetId}/query`, { sql });
  return response.data;
};

export const parseNaturalLanguageQuery = async (datasetId: string, query: string) => {
  const response = await api.post(`/api/v1/nl/${datasetId}/query`, { query });
  return response.data;
};

export const executeNaturalLanguageQuery = async (datasetId: string, parsedQuery: any) => {
  const response = await api.post(`/api/v1/nl/${datasetId}/execute`, { parsed_query: parsedQuery });
  return response.data;
};

export const modifyChart = async (datasetId: string, query: string, existingChart: any) => {
  const response = await api.post(`/api/v1/nl/${datasetId}/modify_chart`, { 
    query, 
    existing_chart: existingChart 
  });
  return response.data;
};

export const applyChartModification = async (datasetId: string, modificationPlan: any) => {
  const response = await api.post(`/api/v1/nl/${datasetId}/apply_modification`, { 
    modification_plan: modificationPlan 
  });
  return response.data;
};

export const addChartToDashboard = async (datasetId: string, chartConfig: any) => {
  const response = await api.post(`/api/v1/nl/${datasetId}/add_to_dashboard`, { 
    chart_config: chartConfig 
  });
  return response.data;
};

export const deleteDataset = async (datasetId: string) => {
  const response = await api.delete(`/api/v1/upload/${datasetId}`);
  return response.data;
};

export const validateCSV = async (file: File) => {
  const formData = new FormData();
  formData.append('file', file);
  
  const response = await api.post('/api/v1/upload/validate', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  });
  
  return response.data;
};