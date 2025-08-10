/**
 * Dashboard page for displaying generated analytics
 */

'use client';

import React, { useState, useEffect } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { ArrowLeft, AlertCircle, Loader2 } from 'lucide-react';
import { Dashboard } from '@/components/dashboard/Dashboard';
import { Button } from '@/components/ui/Button';
import { DashboardResponse, ProcessingStatus } from '@/types';
import { getDashboard, getProcessingStatus } from '@/lib/api';

export default function DashboardPage() {
  const params = useParams();
  const router = useRouter();
  const datasetId = params.datasetId as string;

  const [dashboardData, setDashboardData] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [processingStatus, setProcessingStatus] = useState<ProcessingStatus | null>(null);

  useEffect(() => {
    if (!datasetId) {
      setError('No dataset ID provided');
      setLoading(false);
      return;
    }

    loadDashboard();
  }, [datasetId]);

  const loadDashboard = async () => {
    try {
      setLoading(true);
      setError(null);

      // First check if processing is complete
      const status = await getProcessingStatus(datasetId);
      setProcessingStatus(status.status);

      if (status.status !== ProcessingStatus.COMPLETED) {
        if (status.status === ProcessingStatus.FAILED) {
          setError(status.error_details || 'Data processing failed');
        } else {
          setError('Dashboard is not ready yet. Please wait for processing to complete.');
        }
        setLoading(false);
        return;
      }

      // Load dashboard data
      const data = await getDashboard(datasetId);
      setDashboardData(data);

    } catch (err: any) {
      console.error('Error loading dashboard:', err);
      
      if (err.response?.status === 404) {
        setError('Dashboard not found. The dataset may not exist or processing may not be complete.');
      } else if (err.response?.status === 202) {
        setError('Dashboard is still being generated. Please wait a moment and try again.');
      } else {
        setError(err.response?.data?.detail || err.message || 'Failed to load dashboard');
      }
    } finally {
      setLoading(false);
    }
  };

  const handleError = (errorMessage: string) => {
    console.error('Dashboard error:', errorMessage);
    setError(errorMessage);
  };

  const handleRetry = () => {
    loadDashboard();
  };

  const handleBackToHome = () => {
    router.push('/');
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="h-12 w-12 text-blue-600 animate-spin mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-gray-900 mb-2">Loading Dashboard</h2>
          <p className="text-gray-600">Please wait while we load your analytics dashboard...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="max-w-md w-full bg-white rounded-lg shadow-lg p-8 text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-gray-900 mb-2">
            Unable to Load Dashboard
          </h2>
          <p className="text-gray-600 mb-6 leading-relaxed">
            {error}
          </p>
          
          <div className="space-y-3">
            {processingStatus !== ProcessingStatus.FAILED && (
              <Button onClick={handleRetry} className="w-full">
                Try Again
              </Button>
            )}
            
            <Button 
              onClick={handleBackToHome} 
              variant="outline" 
              className="w-full"
            >
              <ArrowLeft className="h-4 w-4 mr-2" />
              Back to Home
            </Button>
          </div>

          {processingStatus && processingStatus !== ProcessingStatus.COMPLETED && (
            <div className="mt-6 p-4 bg-blue-50 rounded-lg">
              <p className="text-sm text-blue-700">
                <strong>Status:</strong> {processingStatus}
              </p>
              <p className="text-xs text-blue-600 mt-1">
                Processing typically takes 30-60 seconds. You can refresh this page or wait for automatic updates.
              </p>
            </div>
          )}
        </div>
      </div>
    );
  }

  if (!dashboardData) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-gray-500 mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-gray-900 mb-2">No Dashboard Data</h2>
          <p className="text-gray-600 mb-6">
            Dashboard data could not be loaded for this dataset.
          </p>
          <Button onClick={handleBackToHome} variant="outline">
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Home
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Navigation */}
      <nav className="bg-white border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <Button
              onClick={handleBackToHome}
              variant="ghost"
              size="sm"
              className="flex items-center space-x-2"
            >
              <ArrowLeft className="h-4 w-4" />
              <span>Back to Home</span>
            </Button>

            <div className="text-sm text-gray-500">
              Dataset: {datasetId.slice(0, 8)}...
            </div>
          </div>
        </div>
      </nav>

      {/* Dashboard Content */}
      <Dashboard
        config={dashboardData.dashboard_config}
        datasetId={datasetId}
        onError={handleError}
      />

      {/* Footer with metadata */}
      <footer className="bg-white border-t border-gray-200 mt-8">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between">
            <div className="text-sm text-gray-500">
              <p>
                Generated at: {new Date(dashboardData.generated_at).toLocaleString()}
              </p>
              <p className="mt-1">
                Domain: {dashboardData.domain_info.domain} 
                {' '}({Math.round(dashboardData.domain_info.confidence * 100)}% confidence)
              </p>
            </div>
            
            <div className="mt-4 sm:mt-0 text-sm text-gray-400">
              <p>
                {dashboardData.profile.total_rows.toLocaleString()} rows • {' '}
                {dashboardData.profile.total_columns} columns
              </p>
            </div>
          </div>

          {dashboardData.domain_info.reasoning && (
            <div className="mt-4 p-4 bg-gray-50 rounded-lg">
              <h4 className="text-sm font-medium text-gray-900 mb-2">
                Why this dashboard was generated:
              </h4>
              <p className="text-sm text-gray-600 leading-relaxed">
                {dashboardData.domain_info.reasoning}
              </p>
            </div>
          )}
        </div>
      </footer>
    </div>
  );
}