/**
 * KPI Card component for displaying key performance indicators
 */

'use client';

import React, { useMemo } from 'react';
import { TrendingUp, TrendingDown, Minus, DollarSign, Users, Package, BarChart3 } from 'lucide-react';
import { KPIConfig, QueryResult } from '@/types';
import { formatNumber, formatCurrency, formatPercentage } from '@/lib/utils';

interface KPICardProps {
  config: KPIConfig;
  data?: QueryResult;
  previousData?: QueryResult;
  loading?: boolean;
  error?: string;
}

export function KPICard({ config, data, previousData, loading = false, error }: KPICardProps) {
  const { value, trend, trendPercentage } = useMemo(() => {
    if (!data || !data.data || data.data.length === 0) {
      return { value: 0, trend: null, trendPercentage: 0 };
    }

    // Calculate the main value based on configuration
    let calculatedValue = 0;
    const column = config.value_column;

    if (column === '*') {
      // Count all records
      calculatedValue = data.data.length;
    } else {
      const values = data.data
        .map(row => row[column])
        .filter(val => val !== null && val !== undefined && !isNaN(Number(val)))
        .map(val => Number(val));

      switch (config.calculation.toLowerCase()) {
        case 'sum':
          calculatedValue = values.reduce((sum, val) => sum + val, 0);
          break;
        case 'avg':
        case 'mean':
          calculatedValue = values.length > 0 ? values.reduce((sum, val) => sum + val, 0) / values.length : 0;
          break;
        case 'count':
          calculatedValue = values.length;
          break;
        case 'max':
          calculatedValue = values.length > 0 ? Math.max(...values) : 0;
          break;
        case 'min':
          calculatedValue = values.length > 0 ? Math.min(...values) : 0;
          break;
        default:
          calculatedValue = values.reduce((sum, val) => sum + val, 0);
      }
    }

    // Calculate trend if previous data is available
    let trend = null;
    let trendPercentage = 0;

    if (previousData && previousData.data && previousData.data.length > 0) {
      // Calculate previous value using the same logic
      let previousValue = 0;
      
      if (column === '*') {
        previousValue = previousData.data.length;
      } else {
        const previousValues = previousData.data
          .map(row => row[column])
          .filter(val => val !== null && val !== undefined && !isNaN(Number(val)))
          .map(val => Number(val));

        switch (config.calculation.toLowerCase()) {
          case 'sum':
            previousValue = previousValues.reduce((sum, val) => sum + val, 0);
            break;
          case 'avg':
          case 'mean':
            previousValue = previousValues.length > 0 ? previousValues.reduce((sum, val) => sum + val, 0) / previousValues.length : 0;
            break;
          case 'count':
            previousValue = previousValues.length;
            break;
          case 'max':
            previousValue = previousValues.length > 0 ? Math.max(...previousValues) : 0;
            break;
          case 'min':
            previousValue = previousValues.length > 0 ? Math.min(...previousValues) : 0;
            break;
          default:
            previousValue = previousValues.reduce((sum, val) => sum + val, 0);
        }
      }

      if (previousValue !== 0) {
        trendPercentage = ((calculatedValue - previousValue) / previousValue) * 100;
        trend = trendPercentage > 0 ? 'up' : trendPercentage < 0 ? 'down' : 'neutral';
      }
    }

    return { value: calculatedValue, trend, trendPercentage };
  }, [data, previousData, config]);

  const formatValue = (val: number) => {
    switch (config.format_type) {
      case 'currency':
        return formatCurrency(val);
      case 'percentage':
        return formatPercentage(val / 100);
      case 'number':
        return formatNumber(val);
      default:
        return formatNumber(val);
    }
  };

  const getIcon = () => {
    const iconProps = { className: "h-5 w-5" };
    
    if (config.icon) {
      // Map icon names to actual icons
      const iconMap: Record<string, React.ReactNode> = {
        'dollar-sign': <DollarSign {...iconProps} />,
        'users': <Users {...iconProps} />,
        'package': <Package {...iconProps} />,
        'bar-chart': <BarChart3 {...iconProps} />,
      };
      
      return iconMap[config.icon] || <BarChart3 {...iconProps} />;
    }

    // Default icons based on format type
    switch (config.format_type) {
      case 'currency':
        return <DollarSign {...iconProps} />;
      case 'percentage':
        return <BarChart3 {...iconProps} />;
      default:
        return <BarChart3 {...iconProps} />;
    }
  };

  const getTrendIcon = () => {
    const iconProps = { className: "h-4 w-4" };
    
    switch (trend) {
      case 'up':
        return <TrendingUp {...iconProps} />;
      case 'down':
        return <TrendingDown {...iconProps} />;
      default:
        return <Minus {...iconProps} />;
    }
  };

  const getTrendColor = () => {
    switch (trend) {
      case 'up':
        return 'text-green-600';
      case 'down':
        return 'text-red-600';
      default:
        return 'text-gray-500';
    }
  };

  if (loading) {
    return (
      <div className="bg-white border border-gray-200 rounded-lg p-6 animate-pulse">
        <div className="flex items-center justify-between mb-4">
          <div className="h-4 bg-gray-200 rounded w-1/2"></div>
          <div className="h-5 w-5 bg-gray-200 rounded"></div>
        </div>
        <div className="h-8 bg-gray-200 rounded w-3/4 mb-2"></div>
        <div className="h-4 bg-gray-200 rounded w-1/3"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white border border-red-200 rounded-lg p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-medium text-gray-900">{config.name}</h3>
          {getIcon()}
        </div>
        <div className="text-red-600 text-sm">
          Error loading KPI: {error}
        </div>
      </div>
    );
  }

  return (
    <div 
      className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-md transition-shadow"
      style={{ borderLeftColor: config.color, borderLeftWidth: '4px' }}
    >
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-gray-900 truncate">
          {config.name}
        </h3>
        <div className="text-gray-400">
          {getIcon()}
        </div>
      </div>

      <div className="space-y-2">
        <div className="text-2xl font-bold text-gray-900">
          {formatValue(value)}
        </div>

        {trend && (
          <div className={`flex items-center space-x-1 text-sm ${getTrendColor()}`}>
            {getTrendIcon()}
            <span>
              {Math.abs(trendPercentage).toFixed(1)}%
            </span>
            <span className="text-gray-500">
              vs previous period
            </span>
          </div>
        )}

        {config.description && (
          <p className="text-xs text-gray-500 leading-relaxed">
            {config.description}
          </p>
        )}
      </div>

      {/* Explanation tooltip - could be enhanced with a proper tooltip component */}
      {config.explanation && (
        <div className="mt-3 pt-3 border-t border-gray-100">
          <p className="text-xs text-gray-400 leading-relaxed">
            {config.explanation}
          </p>
        </div>
      )}
    </div>
  );
}