/**
 * File upload component with drag and drop functionality
 */

'use client';

import React, { useCallback, useState } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload, AlertCircle, CheckCircle2, File } from 'lucide-react';
import { Button } from '@/components/ui/Button';
import { formatFileSize, validateCSVFile } from '@/lib/utils';
import { uploadCSV, validateCSV } from '@/lib/api';
import { FileValidationResponse, UploadResponse } from '@/types';

interface FileUploadProps {
  onUploadStart?: (file: File) => void;
  onUploadSuccess?: (response: UploadResponse) => void;
  onUploadError?: (error: string) => void;
  maxSize?: number;
  sampleSize?: number;
}

export function FileUpload({
  onUploadStart,
  onUploadSuccess,
  onUploadError,
  maxSize = 15 * 1024 * 1024, // 15MB
  sampleSize = 1000
}: FileUploadProps) {
  const [uploading, setUploading] = useState(false);
  const [validating, setValidating] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [validation, setValidation] = useState<FileValidationResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    setError(null);
    
    if (acceptedFiles.length === 0) {
      setError('No files selected or file type not supported');
      return;
    }

    const file = acceptedFiles[0];
    
    // Basic validation
    const clientValidation = validateCSVFile(file);
    if (!clientValidation.valid) {
      setError(clientValidation.error || 'Invalid file');
      return;
    }

    setSelectedFile(file);
    setValidating(true);

    try {
      // Validate file with server
      const validationResult = await validateCSV(file);
      setValidation(validationResult);
      
      if (!validationResult.is_valid) {
        setError(validationResult.error_message || 'File validation failed');
        setSelectedFile(null);
      }
    } catch (err) {
      console.error('File validation error:', err);
      setError('Failed to validate file');
      setSelectedFile(null);
    } finally {
      setValidating(false);
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.ms-excel': ['.csv']
    },
    maxFiles: 1,
    maxSize,
    disabled: uploading || validating
  });

  const handleUpload = async () => {
    if (!selectedFile || !validation?.is_valid) return;

    setUploading(true);
    setError(null);
    onUploadStart?.(selectedFile);

    try {
      const response = await uploadCSV(selectedFile, sampleSize);
      onUploadSuccess?.(response);
      
      // Reset state after successful upload
      setSelectedFile(null);
      setValidation(null);
    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Upload failed';
      setError(errorMessage);
      onUploadError?.(errorMessage);
    } finally {
      setUploading(false);
    }
  };

  const handleRemoveFile = () => {
    setSelectedFile(null);
    setValidation(null);
    setError(null);
  };

  return (
    <div className="w-full max-w-2xl mx-auto space-y-4">
      {/* Drop zone */}
      <div
        {...getRootProps()}
        className={`
          border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors
          ${isDragActive 
            ? 'border-blue-400 bg-blue-50' 
            : 'border-gray-300 hover:border-gray-400'
          }
          ${(uploading || validating) ? 'opacity-50 cursor-not-allowed' : ''}
        `}
      >
        <input {...getInputProps()} />
        
        <div className="flex flex-col items-center space-y-3">
          <Upload className={`h-12 w-12 ${isDragActive ? 'text-blue-500' : 'text-gray-400'}`} />
          
          <div>
            <p className="text-lg font-medium text-gray-900">
              {isDragActive ? 'Drop your CSV file here' : 'Upload your CSV file'}
            </p>
            <p className="text-sm text-gray-500 mt-1">
              Drag and drop or click to browse • Max {formatFileSize(maxSize)}
            </p>
          </div>
        </div>
      </div>

      {/* Error display */}
      {error && (
        <div className="flex items-center space-x-2 p-3 bg-red-50 border border-red-200 rounded-md">
          <AlertCircle className="h-5 w-5 text-red-500 flex-shrink-0" />
          <p className="text-sm text-red-700">{error}</p>
        </div>
      )}

      {/* File info and validation */}
      {selectedFile && (
        <div className="border rounded-lg p-4 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <File className="h-8 w-8 text-blue-500" />
              <div>
                <p className="font-medium text-gray-900">{selectedFile.name}</p>
                <p className="text-sm text-gray-500">{formatFileSize(selectedFile.size)}</p>
              </div>
            </div>
            
            <Button
              variant="ghost"
              size="sm"
              onClick={handleRemoveFile}
              disabled={uploading}
            >
              Remove
            </Button>
          </div>

          {/* Validation status */}
          {validating && (
            <div className="flex items-center space-x-2 text-blue-600">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600"></div>
              <span className="text-sm">Validating file...</span>
            </div>
          )}

          {validation && !validating && (
            <div className="space-y-2">
              {validation.is_valid ? (
                <div className="flex items-center space-x-2 text-green-600">
                  <CheckCircle2 className="h-4 w-4" />
                  <span className="text-sm font-medium">File is valid</span>
                </div>
              ) : (
                <div className="flex items-center space-x-2 text-red-600">
                  <AlertCircle className="h-4 w-4" />
                  <span className="text-sm font-medium">File validation failed</span>
                </div>
              )}

              {/* Validation details */}
              {validation.is_valid && (
                <div className="text-sm text-gray-600 space-y-1">
                  <p>• Estimated {validation.estimated_rows?.toLocaleString()} rows</p>
                  <p>• {validation.estimated_columns} columns</p>
                  {validation.detected_encoding && (
                    <p>• Encoding: {validation.detected_encoding}</p>
                  )}
                  {validation.detected_delimiter && (
                    <p>• Delimiter: '{validation.detected_delimiter}'</p>
                  )}
                </div>
              )}

              {/* Warnings */}
              {validation.warnings && validation.warnings.length > 0 && (
                <div className="text-sm text-amber-600">
                  <p className="font-medium">Warnings:</p>
                  <ul className="list-disc list-inside space-y-1">
                    {validation.warnings.map((warning, index) => (
                      <li key={index}>{warning}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          {/* Upload button */}
          {validation?.is_valid && !uploading && (
            <Button
              onClick={handleUpload}
              loading={uploading}
              className="w-full"
            >
              {uploading ? 'Uploading...' : 'Upload and Analyze'}
            </Button>
          )}
        </div>
      )}
    </div>
  );
}