/**
 * Main dashboard component that displays KPIs and charts
 */

'use client';

import React, { useState, useEffect, useMemo } from 'react';
import { RefreshCw, Filter, Download, Share2, Info, BarChart3 } from 'lucide-react';
import { DashboardConfig, QueryResult, FilterState, DomainType } from '@/types';
import { KPICard } from './KPICard';
import { BaseChart } from '../charts/BaseChart';
import { Button } from '../ui/Button';
import { executeQuery, getDataSample } from '@/lib/api';

interface DashboardProps {
  config: DashboardConfig;
  datasetId: string;
  onError?: (error: string) => void;
}

export function Dashboard({ config, datasetId, onError }: DashboardProps) {
  const [kpiData, setKpiData] = useState<Record<string, QueryResult>>({});
  const [chartData, setChartData] = useState<Record<string, QueryResult>>({});
  const [filters, setFilters] = useState<FilterState>({});
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [showFilters, setShowFilters] = useState(false);

  // Initialize filters with default values
  useEffect(() => {
    const initialFilters: FilterState = {};
    config.filters.forEach(filter => {
      if (filter.default_value !== undefined) {
        initialFilters[filter.id] = filter.default_value;
      }
    });
    setFilters(initialFilters);
  }, [config.filters]);

  // Load data for KPIs and charts
  const loadData = async (showLoader = true) => {
    if (showLoader) setLoading(true);
    setRefreshing(!showLoader);

    try {
      const promises: Promise<any>[] = [];
      const kpiQueries: Record<string, string> = {};
      const chartQueries: Record<string, string> = {};

      // Generate KPI queries
      config.kpis.forEach(kpi => {
        let query = '';
        if (kpi.value_column === '*') {
          query = 'SELECT COUNT(*) as value FROM dataset';
        } else {
          switch (kpi.calculation.toLowerCase()) {
            case 'sum':
              query = `SELECT SUM(${kpi.value_column}) as value FROM dataset`;
              break;
            case 'avg':
            case 'mean':
              query = `SELECT AVG(${kpi.value_column}) as value FROM dataset`;
              break;
            case 'count':
              query = `SELECT COUNT(${kpi.value_column}) as value FROM dataset`;
              break;
            case 'max':
              query = `SELECT MAX(${kpi.value_column}) as value FROM dataset`;
              break;
            case 'min':
              query = `SELECT MIN(${kpi.value_column}) as value FROM dataset`;
              break;
            default:
              query = `SELECT SUM(${kpi.value_column}) as value FROM dataset`;
          }
        }

        // Apply filters to KPI queries
        const whereClause = buildWhereClause(filters);
        if (whereClause) {
          query += ` WHERE ${whereClause}`;
        }

        kpiQueries[kpi.id] = query;
        promises.push(
          executeQuery(datasetId, query)
            .then(result => ({ type: 'kpi', id: kpi.id, result }))
            .catch(error => ({ type: 'kpi', id: kpi.id, error: error.message }))
        );
      });

      // Generate chart queries
      config.charts.forEach(chart => {
        let query = '';
        
        if (chart.x_axis && chart.y_axis) {
          query = `SELECT ${chart.x_axis}`;
          
          if (chart.aggregation && chart.aggregation !== 'none') {
            query += `, ${chart.aggregation.toUpperCase()}(${chart.y_axis}) as ${chart.y_axis}`;
            query += ` FROM dataset`;
            
            // Apply filters
            const whereClause = buildWhereClause(filters);
            if (whereClause) {
              query += ` WHERE ${whereClause}`;
            }
            
            query += ` GROUP BY ${chart.x_axis}`;
          } else {
            query += `, ${chart.y_axis} FROM dataset`;
            
            // Apply filters
            const whereClause = buildWhereClause(filters);
            if (whereClause) {
              query += ` WHERE ${whereClause}`;
            }
          }

          // Apply sorting
          if (chart.sort_by) {
            query += ` ORDER BY ${chart.sort_by} ${chart.sort_order.toUpperCase()}`;
          }

          // Apply limit
          if (chart.limit && chart.limit > 0) {
            query += ` LIMIT ${chart.limit}`;
          }
        } else {
          // Fallback to sample data if no specific axes defined
          query = 'SELECT * FROM dataset LIMIT 100';
        }

        chartQueries[chart.id] = query;
        promises.push(
          executeQuery(datasetId, query)
            .then(result => ({ type: 'chart', id: chart.id, result }))
            .catch(error => ({ type: 'chart', id: chart.id, error: error.message }))
        );
      });

      // Execute all queries
      const results = await Promise.all(promises);

      // Process results
      const newKpiData: Record<string, QueryResult> = {};
      const newChartData: Record<string, QueryResult> = {};

      results.forEach(result => {
        if (result.error) {
          console.error(`Error loading ${result.type} ${result.id}:`, result.error);
          onError?.(`Failed to load ${result.type}: ${result.error}`);
          return;
        }

        if (result.type === 'kpi') {
          newKpiData[result.id] = result.result;
        } else if (result.type === 'chart') {
          newChartData[result.id] = result.result;
        }
      });

      setKpiData(newKpiData);
      setChartData(newChartData);

    } catch (error: any) {
      console.error('Error loading dashboard data:', error);
      onError?.(error.message || 'Failed to load dashboard data');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  // Build WHERE clause from active filters
  const buildWhereClause = (activeFilters: FilterState): string => {
    const conditions: string[] = [];

    Object.entries(activeFilters).forEach(([filterId, value]) => {
      const filter = config.filters.find(f => f.id === filterId);
      if (!filter || value === undefined || value === null) return;

      switch (filter.type) {
        case 'categorical':
        case 'multi_select':
          if (Array.isArray(value) && value.length > 0) {
            const values = value.map(v => `'${v}'`).join(', ');
            conditions.push(`${filter.column} IN (${values})`);
          } else if (typeof value === 'string' && value) {
            conditions.push(`${filter.column} = '${value}'`);
          }
          break;
        case 'numeric_range':
          if (typeof value === 'object' && value.min !== undefined && value.max !== undefined) {
            conditions.push(`${filter.column} BETWEEN ${value.min} AND ${value.max}`);
          }
          break;
        case 'date_range':
          if (typeof value === 'object' && value.start && value.end) {
            conditions.push(`${filter.column} BETWEEN '${value.start}' AND '${value.end}'`);
          }
          break;
      }
    });

    return conditions.join(' AND ');
  };

  // Load data when component mounts or filters change
  useEffect(() => {
    loadData();
  }, [datasetId, config, filters]);

  const handleRefresh = () => {
    loadData(false);
  };

  const getDomainTheme = () => {
    const themes = {
      [DomainType.ECOMMERCE]: 'from-blue-500 to-purple-600',
      [DomainType.FINANCE]: 'from-green-500 to-teal-600',
      [DomainType.MANUFACTURING]: 'from-orange-500 to-red-600',
      [DomainType.SAAS]: 'from-cyan-500 to-blue-600',
      [DomainType.GENERIC]: 'from-gray-500 to-gray-600'
    };
    
    return themes[config.domain as DomainType] || themes[DomainType.GENERIC];
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className={`bg-gradient-to-r ${getDomainTheme()} text-white`}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold">{config.title}</h1>
              <p className="text-white/80 mt-1">{config.description}</p>
            </div>
            
            <div className="flex items-center space-x-3">
              <Button
                onClick={() => setShowFilters(!showFilters)}
                variant="ghost"
                size="sm"
                className="text-white hover:bg-white/20"
              >
                <Filter className="h-4 w-4 mr-2" />
                Filters
              </Button>
              
              <Button
                onClick={handleRefresh}
                variant="ghost"
                size="sm"
                loading={refreshing}
                className="text-white hover:bg-white/20"
              >
                <RefreshCw className="h-4 w-4 mr-2" />
                Refresh
              </Button>
              
              <Button
                variant="ghost"
                size="sm"
                className="text-white hover:bg-white/20"
              >
                <Share2 className="h-4 w-4 mr-2" />
                Share
              </Button>
            </div>
          </div>

          {/* Domain info */}
          <div className="mt-4 flex items-center space-x-2 text-white/80 text-sm">
            <Info className="h-4 w-4" />
            <span>Domain: {config.domain}</span>
            <span>•</span>
            <span>{config.explanation}</span>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {/* Filters */}
        {showFilters && config.filters.length > 0 && (
          <div className="bg-white rounded-lg border border-gray-200 p-4 mb-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Filters</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {config.filters.map(filter => (
                <div key={filter.id}>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    {filter.name}
                  </label>
                  {/* Filter inputs would go here - simplified for now */}
                  <div className="text-sm text-gray-500">
                    {filter.type} filter for {filter.column}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* KPIs */}
        {config.kpis.length > 0 && (
          <div className="mb-8">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              {config.kpis.map(kpi => (
                <KPICard
                  key={kpi.id}
                  config={kpi}
                  data={kpiData[kpi.id]}
                  loading={loading}
                />
              ))}
            </div>
          </div>
        )}

        {/* Charts */}
        {config.charts.length > 0 && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {config.charts.map(chart => (
              <div
                key={chart.id}
                className="bg-white border border-gray-200 rounded-lg p-6"
                style={{
                  gridColumn: chart.width > 6 ? 'span 2' : 'span 1',
                  minHeight: `${chart.height * 60}px`
                }}
              >
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h3 className="text-lg font-medium text-gray-900">
                      {chart.title}
                    </h3>
                    {chart.description && (
                      <p className="text-sm text-gray-500 mt-1">
                        {chart.description}
                      </p>
                    )}
                  </div>
                  <Button variant="ghost" size="sm">
                    <Download className="h-4 w-4" />
                  </Button>
                </div>

                {loading ? (
                  <div className="flex items-center justify-center h-64">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                  </div>
                ) : chartData[chart.id] ? (
                  <BaseChart
                    config={chart}
                    data={chartData[chart.id]}
                    onError={onError}
                  />
                ) : (
                  <div className="flex items-center justify-center h-64 text-gray-500">
                    No data available for this chart
                  </div>
                )}

                {chart.explanation && (
                  <div className="mt-4 pt-4 border-t border-gray-100">
                    <p className="text-xs text-gray-500 leading-relaxed">
                      {chart.explanation}
                    </p>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}

        {/* Empty state */}
        {config.kpis.length === 0 && config.charts.length === 0 && (
          <div className="text-center py-12">
            <div className="text-gray-500">
              <BarChart3 className="h-12 w-12 mx-auto mb-4" />
              <p className="text-lg font-medium">No dashboard components configured</p>
              <p className="text-sm mt-1">The dashboard configuration is empty or failed to load.</p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}