/**
 * Processing status component with progress tracking
 */

'use client';

import React, { useEffect, useState } from 'react';
import { CheckCircle2, AlertCircle, Clock, BarChart3 } from 'lucide-react';
import { ProcessingStatus as Status, ProcessingStatusResponse } from '@/types';
import { getProcessingStatus } from '@/lib/api';
import { Button } from '@/components/ui/Button';

interface ProcessingStatusProps {
  datasetId: string;
  onComplete?: (datasetId: string) => void;
  onError?: (error: string) => void;
  pollInterval?: number;
}

export function ProcessingStatus({
  datasetId,
  onComplete,
  onError,
  pollInterval = 2000
}: ProcessingStatusProps) {
  const [status, setStatus] = useState<ProcessingStatusResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let intervalId: NodeJS.Timeout;
    let isMounted = true;

    const checkStatus = async () => {
      try {
        const response = await getProcessingStatus(datasetId);
        
        if (!isMounted) return;
        
        setStatus(response);
        setError(null);
        
        // Stop polling if processing is complete or failed
        if (response.status === Status.COMPLETED) {
          onComplete?.(datasetId);
          clearInterval(intervalId);
        } else if (response.status === Status.FAILED) {
          onError?.(response.error_details || 'Processing failed');
          clearInterval(intervalId);
        }
      } catch (err: any) {
        if (!isMounted) return;
        
        const errorMessage = err.response?.data?.detail || err.message || 'Failed to get status';
        setError(errorMessage);
        onError?.(errorMessage);
        clearInterval(intervalId);
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    // Initial check
    checkStatus();

    // Set up polling
    intervalId = setInterval(checkStatus, pollInterval);

    return () => {
      isMounted = false;
      clearInterval(intervalId);
    };
  }, [datasetId, onComplete, onError, pollInterval]);

  const getStatusIcon = () => {
    if (loading && !status) {
      return <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>;
    }

    switch (status?.status) {
      case Status.PENDING:
        return <Clock className="h-6 w-6 text-gray-500" />;
      case Status.PROCESSING:
        return <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>;
      case Status.COMPLETED:
        return <CheckCircle2 className="h-6 w-6 text-green-500" />;
      case Status.FAILED:
        return <AlertCircle className="h-6 w-6 text-red-500" />;
      default:
        return <Clock className="h-6 w-6 text-gray-500" />;
    }
  };

  const getStatusColor = () => {
    switch (status?.status) {
      case Status.PENDING:
        return 'text-gray-600';
      case Status.PROCESSING:
        return 'text-blue-600';
      case Status.COMPLETED:
        return 'text-green-600';
      case Status.FAILED:
        return 'text-red-600';
      default:
        return 'text-gray-600';
    }
  };

  const getStatusText = () => {
    if (loading && !status) return 'Checking status...';
    
    switch (status?.status) {
      case Status.PENDING:
        return 'Queued for processing';
      case Status.PROCESSING:
        return 'Processing data';
      case Status.COMPLETED:
        return 'Analysis complete';
      case Status.FAILED:
        return 'Processing failed';
      default:
        return 'Unknown status';
    }
  };

  const formatDuration = (start: string, end?: string) => {
    const startTime = new Date(start);
    const endTime = end ? new Date(end) : new Date();
    const diffMs = endTime.getTime() - startTime.getTime();
    const diffSeconds = Math.floor(diffMs / 1000);
    
    if (diffSeconds < 60) {
      return `${diffSeconds}s`;
    } else {
      const minutes = Math.floor(diffSeconds / 60);
      const seconds = diffSeconds % 60;
      return `${minutes}m ${seconds}s`;
    }
  };

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-center space-x-3">
          <AlertCircle className="h-6 w-6 text-red-500" />
          <div>
            <h3 className="text-lg font-medium text-red-900">Error</h3>
            <p className="text-red-700">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-6 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          {getStatusIcon()}
          <div>
            <h3 className={`text-lg font-medium ${getStatusColor()}`}>
              {getStatusText()}
            </h3>
            {status?.message && (
              <p className="text-sm text-gray-600">{status.message}</p>
            )}
          </div>
        </div>
        
        {status?.status === Status.COMPLETED && (
          <Button
            onClick={() => onComplete?.(datasetId)}
            size="sm"
            className="flex items-center space-x-2"
          >
            <BarChart3 className="h-4 w-4" />
            <span>View Dashboard</span>
          </Button>
        )}
      </div>

      {/* Progress bar */}
      {status?.progress !== undefined && status.status === Status.PROCESSING && (
        <div>
          <div className="flex justify-between text-sm text-gray-600 mb-2">
            <span>Progress</span>
            <span>{Math.round(status.progress * 100)}%</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${status.progress * 100}%` }}
            />
          </div>
        </div>
      )}

      {/* Timing information */}
      {status && (
        <div className="flex justify-between text-sm text-gray-500">
          <span>
            Started: {new Date(status.created_at).toLocaleTimeString()}
          </span>
          <span>
            Duration: {formatDuration(status.created_at, status.completion_time)}
          </span>
        </div>
      )}

      {/* Error details */}
      {status?.status === Status.FAILED && status.error_details && (
        <div className="bg-red-50 border border-red-200 rounded p-3">
          <p className="text-sm text-red-700">
            <strong>Error details:</strong> {status.error_details}
          </p>
        </div>
      )}

      {/* Processing steps */}
      {status?.status === Status.PROCESSING && (
        <div className="space-y-2">
          <h4 className="text-sm font-medium text-gray-900">Processing Steps:</h4>
          <div className="space-y-1 text-sm text-gray-600">
            <div className="flex items-center space-x-2">
              <CheckCircle2 className="h-4 w-4 text-green-500" />
              <span>Data validation and loading</span>
            </div>
            <div className={`flex items-center space-x-2 ${
              (status.progress || 0) > 0.3 ? 'text-green-600' : 'text-gray-400'
            }`}>
              {(status.progress || 0) > 0.3 ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : (
                <div className="h-4 w-4 border-2 border-gray-300 rounded-full" />
              )}
              <span>Data profiling and analysis</span>
            </div>
            <div className={`flex items-center space-x-2 ${
              (status.progress || 0) > 0.5 ? 'text-green-600' : 'text-gray-400'
            }`}>
              {(status.progress || 0) > 0.5 ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : (
                <div className="h-4 w-4 border-2 border-gray-300 rounded-full" />
              )}
              <span>Domain detection</span>
            </div>
            <div className={`flex items-center space-x-2 ${
              (status.progress || 0) > 0.7 ? 'text-green-600' : 'text-gray-400'
            }`}>
              {(status.progress || 0) > 0.7 ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : (
                <div className="h-4 w-4 border-2 border-gray-300 rounded-full" />
              )}
              <span>Dashboard generation</span>
            </div>
            <div className={`flex items-center space-x-2 ${
              (status.progress || 0) >= 1.0 ? 'text-green-600' : 'text-gray-400'
            }`}>
              {(status.progress || 0) >= 1.0 ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : (
                <div className="h-4 w-4 border-2 border-gray-300 rounded-full" />
              )}
              <span>Finalizing results</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}