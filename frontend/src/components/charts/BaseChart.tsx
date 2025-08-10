/**
 * Base chart component that handles different chart types
 */

'use client';

import React, { useMemo } from 'react';
import dynamic from 'next/dynamic';
import { ChartType, ChartConfig, QueryResult } from '@/types';
import { generateColors } from '@/lib/utils';

// Dynamically import Plotly to avoid SSR issues
const Plot = dynamic(() => import('react-plotly.js'), { ssr: false });

// Import Recharts components
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ScatterChart,
  Scatter
} from 'recharts';

interface BaseChartProps {
  config: ChartConfig;
  data: QueryResult;
  width?: number;
  height?: number;
  onError?: (error: string) => void;
}

export function BaseChart({ config, data, width = 400, height = 300, onError }: BaseChartProps) {
  const chartData = useMemo(() => {
    if (!data || !data.data || data.data.length === 0) {
      return [];
    }

    // Transform data based on chart configuration
    let transformedData = data.data;

    // Apply sorting if specified
    if (config.sort_by && transformedData.length > 0) {
      transformedData = [...transformedData].sort((a, b) => {
        const aVal = a[config.sort_by!];
        const bVal = b[config.sort_by!];
        
        if (typeof aVal === 'number' && typeof bVal === 'number') {
          return config.sort_order === 'desc' ? bVal - aVal : aVal - bVal;
        }
        
        return config.sort_order === 'desc' 
          ? String(bVal).localeCompare(String(aVal))
          : String(aVal).localeCompare(String(bVal));
      });
    }

    // Apply limit if specified
    if (config.limit && config.limit > 0) {
      transformedData = transformedData.slice(0, config.limit);
    }

    return transformedData;
  }, [data, config]);

  const colors = useMemo(() => generateColors(chartData.length), [chartData.length]);

  const renderRechartsChart = () => {
    if (chartData.length === 0) {
      return (
        <div className="flex items-center justify-center h-full text-gray-500">
          No data available
        </div>
      );
    }

    const commonProps = {
      data: chartData,
      margin: { top: 20, right: 30, left: 20, bottom: 20 }
    };

    switch (config.type) {
      case ChartType.BAR:
        return (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart {...commonProps}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey={config.x_axis} 
                tick={{ fontSize: 12 }}
                angle={-45}
                textAnchor="end"
                height={60}
              />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              <Bar 
                dataKey={config.y_axis} 
                fill={colors[0]}
                name={config.y_axis}
              />
            </BarChart>
          </ResponsiveContainer>
        );

      case ChartType.LINE:
        return (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart {...commonProps}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey={config.x_axis} 
                tick={{ fontSize: 12 }}
              />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              <Line 
                type="monotone" 
                dataKey={config.y_axis} 
                stroke={colors[0]}
                strokeWidth={2}
                dot={{ r: 4 }}
                name={config.y_axis}
              />
            </LineChart>
          </ResponsiveContainer>
        );

      case ChartType.PIE:
        return (
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name}: ${((percent || 0) * 100).toFixed(0)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey={config.y_axis}
                nameKey={config.x_axis}
              >
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        );

      case ChartType.SCATTER:
        return (
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart {...commonProps}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                type="number" 
                dataKey={config.x_axis} 
                tick={{ fontSize: 12 }}
                name={config.x_axis}
              />
              <YAxis 
                type="number" 
                dataKey={config.y_axis} 
                tick={{ fontSize: 12 }}
                name={config.y_axis}
              />
              <Tooltip cursor={{ strokeDasharray: '3 3' }} />
              <Scatter 
                name={`${config.x_axis} vs ${config.y_axis}`} 
                data={chartData} 
                fill={colors[0]} 
              />
            </ScatterChart>
          </ResponsiveContainer>
        );

      default:
        return (
          <div className="flex items-center justify-center h-full text-gray-500">
            Chart type not supported: {config.type}
          </div>
        );
    }
  };

  const renderPlotlyChart = () => {
    if (chartData.length === 0) {
      return (
        <div className="flex items-center justify-center h-full text-gray-500">
          No data available
        </div>
      );
    }

    try {
      const plotData: any[] = [];
      const layout: any = {
        title: {
          text: config.title,
          font: { size: 14 }
        },
        margin: { t: 50, r: 50, b: 50, l: 50 },
        showlegend: true,
        autosize: true
      };

      switch (config.type) {
        case ChartType.HISTOGRAM:
          plotData.push({
            x: chartData.map(d => d[config.x_axis!]),
            type: 'histogram',
            marker: { color: colors[0] },
            name: config.x_axis
          });
          break;

        case ChartType.HEATMAP:
          // This would need more complex data transformation for heatmaps
          plotData.push({
            z: chartData.map(d => [d[config.x_axis!], d[config.y_axis!]]),
            type: 'heatmap',
            colorscale: 'Viridis'
          });
          break;

        default:
          onError?.(`Plotly chart type not implemented: ${config.type}`);
          return null;
      }

      return (
        <Plot
          data={plotData}
          layout={layout}
          style={{ width: '100%', height: '100%' }}
          useResizeHandler={true}
          config={{
            displayModeBar: false,
            responsive: true
          }}
        />
      );
    } catch (error) {
      console.error('Plotly chart error:', error);
      onError?.(`Failed to render chart: ${error}`);
      return null;
    }
  };

  // Determine which chart library to use
  const useplotly = [ChartType.HISTOGRAM, ChartType.HEATMAP, ChartType.FUNNEL, ChartType.GAUGE]
    .includes(config.type);

  return (
    <div className="w-full h-full min-h-[300px]">
      {useplotly ? renderPlotlyChart() : renderRechartsChart()}
    </div>
  );
}