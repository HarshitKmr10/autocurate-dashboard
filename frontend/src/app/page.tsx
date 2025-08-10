/**
 * Home page with file upload and dashboard access
 */

'use client';

import React, { useState } from 'react';
import { useRouter } from 'next/navigation';
import { Upload, BarChart3, Zap, Brain, Gauge } from 'lucide-react';
import { FileUpload } from '@/components/upload/FileUpload';
import { ProcessingStatus } from '@/components/upload/ProcessingStatus';
import { UploadResponse, ProcessingStatus as Status } from '@/types';

export default function HomePage() {
  const router = useRouter();
  const [uploadResponse, setUploadResponse] = useState<UploadResponse | null>(null);
  const [showProcessing, setShowProcessing] = useState(false);

  const handleUploadStart = (file: File) => {
    console.log('Upload started for:', file.name);
  };

  const handleUploadSuccess = (response: UploadResponse) => {
    console.log('Upload successful:', response);
    setUploadResponse(response);
    setShowProcessing(true);
  };

  const handleUploadError = (error: string) => {
    console.error('Upload error:', error);
    // Error is already displayed by the FileUpload component
  };

  const handleProcessingComplete = (datasetId: string) => {
    console.log('Processing complete for:', datasetId);
    router.push(`/dashboard/${datasetId}`);
  };

  const handleProcessingError = (error: string) => {
    console.error('Processing error:', error);
    setShowProcessing(false);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50">
      {/* Header */}
      <header className="relative overflow-hidden bg-white/80 backdrop-blur-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <div className="h-10 w-10 bg-gradient-to-br from-blue-600 to-purple-600 rounded-lg flex items-center justify-center">
                <BarChart3 className="h-6 w-6 text-white" />
              </div>
              <div>
                <h1 className="text-2xl font-bold text-gray-900">Autocurate</h1>
                <p className="text-sm text-gray-600">AI-Powered Analytics Dashboard</p>
              </div>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        {!showProcessing ? (
          <>
            {/* Hero Section */}
            <div className="text-center mb-16">
              <h2 className="text-4xl font-bold text-gray-900 mb-4">
                Transform Your Data Into
                <span className="bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
                  {' '}Intelligent Dashboards
                </span>
              </h2>
              <p className="text-xl text-gray-600 max-w-3xl mx-auto leading-relaxed">
                Simply upload your CSV file and watch as our AI analyzes your data, 
                detects the business context, and generates a customized dashboard 
                with relevant KPIs and visualizations.
              </p>
            </div>

            {/* Features */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-16">
              <div className="text-center p-6">
                <div className="h-16 w-16 bg-blue-100 rounded-full flex items-center justify-center mx-auto mb-4">
                  <Brain className="h-8 w-8 text-blue-600" />
                </div>
                <h3 className="text-lg font-semibold text-gray-900 mb-2">AI-Powered Analysis</h3>
                <p className="text-gray-600">
                  Our AI automatically detects your data&apos;s business domain and suggests 
                  relevant metrics and visualizations.
                </p>
              </div>
              
              <div className="text-center p-6">
                <div className="h-16 w-16 bg-purple-100 rounded-full flex items-center justify-center mx-auto mb-4">
                  <Zap className="h-8 w-8 text-purple-600" />
                </div>
                <h3 className="text-lg font-semibold text-gray-900 mb-2">Zero Configuration</h3>
                <p className="text-gray-600">
                  No manual setup required. Just upload your CSV and get a 
                  fully interactive dashboard in seconds.
                </p>
              </div>
              
              <div className="text-center p-6">
                <div className="h-16 w-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-4">
                  <Gauge className="h-8 w-8 text-green-600" />
                </div>
                <h3 className="text-lg font-semibold text-gray-900 mb-2">Real-time Insights</h3>
                <p className="text-gray-600">
                  Interactive charts, smart filters, and key performance 
                  indicators tailored to your specific domain.
                </p>
              </div>
            </div>

            {/* Upload Section */}
            <div className="bg-white rounded-2xl shadow-xl border border-gray-200 p-8 mb-12">
              <div className="text-center mb-8">
                <Upload className="h-12 w-12 text-blue-600 mx-auto mb-4" />
                <h3 className="text-2xl font-bold text-gray-900 mb-2">Get Started</h3>
                <p className="text-gray-600">
                  Upload your CSV file to begin the analysis. We support files up to 15MB.
                </p>
              </div>

              <FileUpload
                onUploadStart={handleUploadStart}
                onUploadSuccess={handleUploadSuccess}
                onUploadError={handleUploadError}
                sampleSize={1000}
              />
            </div>

            {/* Supported Domains */}
            <div className="text-center">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">
                Automatically Detects Business Domains
              </h3>
              <div className="flex flex-wrap justify-center gap-3">
                {[
                  'E-commerce',
                  'Finance',
                  'Manufacturing', 
                  'SaaS',
                  'Marketing',
                  'Sales',
                  'Operations',
                  'And More...'
                ].map((domain) => (
                  <span
                    key={domain}
                    className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm font-medium"
                  >
                    {domain}
                  </span>
                ))}
              </div>
            </div>
          </>
        ) : (
          /* Processing Section */
          <div className="max-w-2xl mx-auto">
            <div className="text-center mb-8">
              <div className="h-16 w-16 bg-blue-100 rounded-full flex items-center justify-center mx-auto mb-4">
                <Brain className="h-8 w-8 text-blue-600" />
              </div>
              <h2 className="text-2xl font-bold text-gray-900 mb-2">
                Analyzing Your Data
              </h2>
              <p className="text-gray-600">
                Our AI is processing your data and generating your custom dashboard...
              </p>
            </div>

            {uploadResponse && (
              <ProcessingStatus
                datasetId={uploadResponse.dataset_id}
                onComplete={handleProcessingComplete}
                onError={handleProcessingError}
                pollInterval={2000}
              />
            )}

            <div className="text-center mt-8">
              <button
                onClick={() => setShowProcessing(false)}
                className="text-blue-600 hover:text-blue-700 text-sm font-medium"
              >
                ← Back to upload
              </button>
            </div>
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="bg-gray-50 border-t border-gray-200 mt-16">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="text-center text-gray-500 text-sm">
            <p>© 2024 Autocurate Analytics Dashboard. Powered by AI.</p>
          </div>
        </div>
      </footer>
    </div>
  );
}