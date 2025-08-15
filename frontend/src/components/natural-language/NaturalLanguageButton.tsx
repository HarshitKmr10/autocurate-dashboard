/**
 * Natural Language Button Component
 */

"use client";

import React, { useState } from 'react';
import { MessageSquare, Plus, Edit3 } from 'lucide-react';
import { NaturalLanguageInterface } from './NaturalLanguageInterface';
import { ChartConfig, QueryResult } from '@/types';

interface NaturalLanguageButtonProps {
  datasetId: string;
  mode?: 'create' | 'modify';
  existingChart?: ChartConfig;
  onChartCreated?: (chartConfig: ChartConfig, data: QueryResult) => void;
  onChartModified?: (originalChart: ChartConfig, newChart: ChartConfig, data: QueryResult) => void;
  buttonText?: string;
  className?: string;
  variant?: 'primary' | 'secondary' | 'ghost';
  size?: 'sm' | 'md' | 'lg';
}

export function NaturalLanguageButton({
  datasetId,
  mode = 'create',
  existingChart,
  onChartCreated,
  onChartModified,
  buttonText,
  className = '',
  variant = 'primary',
  size = 'md'
}: NaturalLanguageButtonProps) {
  const [isOpen, setIsOpen] = useState(false);

  const getVariantClasses = () => {
    switch (variant) {
      case 'primary':
        return 'bg-blue-500 hover:bg-blue-600 text-white';
      case 'secondary':
        return 'bg-gray-100 hover:bg-gray-200 text-gray-700 border border-gray-300';
      case 'ghost':
        return 'text-gray-600 hover:text-gray-800 hover:bg-gray-100';
      default:
        return 'bg-blue-500 hover:bg-blue-600 text-white';
    }
  };

  const getSizeClasses = () => {
    switch (size) {
      case 'sm':
        return 'px-2 py-1 text-xs';
      case 'md':
        return 'px-3 py-2 text-sm';
      case 'lg':
        return 'px-4 py-3 text-base';
      default:
        return 'px-3 py-2 text-sm';
    }
  };

  const getIcon = () => {
    switch (mode) {
      case 'create':
        return <Plus className="w-4 h-4" />;
      case 'modify':
        return <Edit3 className="w-4 h-4" />;
      default:
        return <MessageSquare className="w-4 h-4" />;
    }
  };

  const getDefaultText = () => {
    switch (mode) {
      case 'create':
        return 'Create with AI';
      case 'modify':
        return 'Modify with AI';
      default:
        return 'Natural Language';
    }
  };

  const handleClose = () => {
    setIsOpen(false);
  };

  const handleChartCreated = (chartConfig: ChartConfig, data: QueryResult) => {
    onChartCreated?.(chartConfig, data);
    setIsOpen(false);
  };

  const handleChartModified = (originalChart: ChartConfig, newChart: ChartConfig, data: QueryResult) => {
    onChartModified?.(originalChart, newChart, data);
    setIsOpen(false);
  };

  return (
    <>
      <button
        onClick={() => setIsOpen(true)}
        className={`
          inline-flex items-center gap-2 rounded-lg font-medium transition-colors
          ${getVariantClasses()}
          ${getSizeClasses()}
          ${className}
        `}
      >
        {getIcon()}
        {buttonText || getDefaultText()}
      </button>

      {isOpen && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg shadow-xl max-w-4xl w-full h-[80vh] flex flex-col">
            <NaturalLanguageInterface
              datasetId={datasetId}
              mode={mode}
              existingChart={existingChart}
              onChartCreated={handleChartCreated}
              onChartModified={handleChartModified}
              onClose={handleClose}
            />
          </div>
        </div>
      )}
    </>
  );
}
