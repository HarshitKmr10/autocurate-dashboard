/**
 * Main dashboard component that displays KPIs and charts
 */

"use client";

import React, { useState, useEffect, useMemo } from "react";
import {
  RefreshCw,
  Filter,
  Download,
  Share2,
  Info,
  BarChart3,
} from "lucide-react";
import { DashboardConfig, QueryResult, FilterState, DomainType } from "@/types";
import { KPICard } from "./KPICard";
import { BaseChart } from "../charts/BaseChart";
import { Button } from "../ui/Button";
import { executeQuery, getDataSample } from "@/lib/api";
import { FilterComponent } from "./FilterComponent";

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
    config.filters.forEach((filter) => {
      if (filter.default_value !== undefined) {
        initialFilters[filter.id] = filter.default_value;
      }
    });
    setFilters(initialFilters);
  }, [config.filters]);

  // Helper to count active filters
  const activeFilterCount = useMemo(() => {
    return Object.values(filters).filter((value) => {
      if (value === null || value === undefined) return false;
      if (Array.isArray(value)) return value.length > 0;
      if (typeof value === "object")
        return Object.values(value).some((v) => v !== null && v !== undefined);
      return true;
    }).length;
  }, [filters]);

  // Load data for KPIs and charts
  const loadData = async (showLoader = true) => {
    if (showLoader) setLoading(true);
    setRefreshing(!showLoader);

    try {
      const promises: Promise<any>[] = [];
      const kpiQueries: Record<string, string> = {};
      const chartQueries: Record<string, string> = {};

      // Generate KPI queries
      config.kpis.forEach((kpi) => {
        let query = "";
        // Check if KPI has a custom SQL query from LLM
        if ((kpi as any).sql_query) {
          // Use LLM query and apply filters while keeping the query starting with SELECT
          query = (kpi as any).sql_query;
          const whereClause = buildWhereClause(filters);
          if (whereClause) {
            query = applyFiltersToDatasetReferences(query, whereClause);
          }
        } else {
          // Fallback to the old logic for backward compatibility
          if (
            kpi.value_column === "*" ||
            kpi.calculation.toLowerCase() === "count"
          ) {
            // Use COUNT for wildcard or explicit count calculations
            if (kpi.value_column === "*") {
              query = "SELECT COUNT(*) as value FROM dataset";
            } else {
              query = `SELECT COUNT(${kpi.value_column}) as value FROM dataset`;
            }
          } else if (
            kpi.format_type === "percentage" &&
            kpi.name.toLowerCase().includes("rate")
          ) {
            // Handle percentage-based KPIs (like cancellation rate, conversion rate, etc.)
            if (kpi.name.toLowerCase().includes("cancel")) {
              // Cancellation rate: percentage of cancelled orders
              query = `SELECT (COUNT(CASE WHEN ${kpi.value_column} = 'Cancelled' THEN 1 END) * 100.0 / COUNT(*)) as value FROM dataset`;
            } else if (kpi.name.toLowerCase().includes("conversion")) {
              // Conversion rate: percentage of completed orders
              query = `SELECT (COUNT(CASE WHEN ${kpi.value_column} = 'Completed' THEN 1 END) * 100.0 / COUNT(*)) as value FROM dataset`;
            } else {
              // Generic percentage calculation - count non-null values as percentage
              query = `SELECT (COUNT(${kpi.value_column}) * 100.0 / COUNT(*)) as value FROM dataset`;
            }
          } else {
            // Use the calculation specified by the backend (which has been validated)
            switch (kpi.calculation.toLowerCase()) {
              case "sum":
                query = `SELECT SUM(${kpi.value_column}) as value FROM dataset`;
                break;
              case "avg":
              case "average":
              case "mean":
                query = `SELECT AVG(${kpi.value_column}) as value FROM dataset`;
                break;
              case "max":
                query = `SELECT MAX(${kpi.value_column}) as value FROM dataset`;
                break;
              case "min":
                query = `SELECT MIN(${kpi.value_column}) as value FROM dataset`;
                break;
              default:
                // Default to COUNT instead of SUM to avoid errors
                query = `SELECT COUNT(${kpi.value_column}) as value FROM dataset`;
            }
          }
        }

        // Apply filters to KPI queries only if not using custom SQL
        if (!(kpi as any).sql_query) {
          const whereClause = buildWhereClause(filters);
          if (whereClause) {
            query += ` WHERE ${whereClause}`;
          }
        }
        kpiQueries[kpi.id] = query;
        
        promises.push(
          executeQuery(datasetId, query)
            .then((result) => ({ type: "kpi", id: kpi.id, result }))
            .catch((error) => {
              console.error(`❌ KPI ${kpi.id} error:`, error);
              
              // If LLM query failed, try a simple fallback
              if ((kpi as any).sql_query && (error.message.includes('syntax error') || error.message.includes('Binder Error') || error.message.includes('GROUP BY') || error.message.includes('not found in FROM clause'))) {
                console.log(`🔄 Attempting fallback query for KPI ${kpi.id}`);
                
                // Generate simple fallback based on KPI type
                let fallbackQuery = '';
                if (kpi.calculation.toLowerCase() === 'sum') {
                  fallbackQuery = `SELECT COALESCE(SUM(${kpi.value_column}), 0) as value FROM dataset`;
                } else if (kpi.calculation.toLowerCase() === 'avg') {
                  fallbackQuery = `SELECT COALESCE(AVG(${kpi.value_column}), 0) as value FROM dataset`;
                } else if (kpi.calculation.toLowerCase() === 'count') {
                  fallbackQuery = `SELECT COUNT(*) as value FROM dataset`;
                } else if (kpi.calculation.toLowerCase() === 'percentage') {
                  fallbackQuery = `SELECT COUNT(*) as value FROM dataset`; // Simple count as fallback
                } else {
                  fallbackQuery = `SELECT COUNT(*) as value FROM dataset`;
                }
                
                // Apply filters to fallback query
                const whereClause = buildWhereClause(filters);
                if (whereClause) {
                  fallbackQuery += ` WHERE ${whereClause}`;
                }
                
                console.log(`📊 KPI ${kpi.id} fallback query:`, fallbackQuery);
                
                return executeQuery(datasetId, fallbackQuery)
                  .then((result) => {
                    console.log(`✅ KPI ${kpi.id} fallback result:`, result);
                    return { type: "kpi", id: kpi.id, result };
                  })
                  .catch((fallbackError) => {
                    console.error(`❌ KPI ${kpi.id} fallback also failed:`, fallbackError);
                    return {
                      type: "kpi",
                      id: kpi.id,
                      error: `LLM query failed: ${error.message}. Fallback failed: ${fallbackError.message}`,
                    };
                  });
              }
              
              return {
                type: "kpi",
                id: kpi.id,
                error: error.message,
              };
            })
        );
      });

              // Generate chart queries
      config.charts.forEach((chart) => {
        let query = "";

        // Check if chart has a custom SQL query from LLM
        if ((chart as any).sql_query) {
          // Use LLM query and apply filters while keeping the query starting with SELECT
          query = (chart as any).sql_query;
          const whereClause = buildWhereClause(filters);
          if (whereClause) {
            query = applyFiltersToDatasetReferences(query, whereClause);
          }
        } else {
          // Fallback to the old logic for backward compatibility
          if (chart.x_axis && chart.y_axis) {
            query = `SELECT ${chart.x_axis}`;

            if (chart.aggregation && chart.aggregation !== "none") {
              // Validate aggregation type to avoid errors
              const validAggregations = ["sum", "avg", "count", "min", "max"];
              const aggregation = validAggregations.includes(
                chart.aggregation.toLowerCase()
              )
                ? chart.aggregation.toUpperCase()
                : "COUNT";

              // Use proper alias to avoid column name conflicts
              const valueAlias = chart.type === 'pie' ? 'count' : 'value';
              query += `, ${aggregation}(${chart.y_axis}) as ${valueAlias}`;
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
              query += ` ORDER BY ${
                chart.sort_by
              } ${chart.sort_order.toUpperCase()}`;
            }

            // Apply limit
            if (chart.limit && chart.limit > 0) {
              query += ` LIMIT ${chart.limit}`;
            }
          } else {
            // Fallback to sample data if no specific axes defined
            query = "SELECT * FROM dataset LIMIT 100";
          }
        }

        chartQueries[chart.id] = query;
        promises.push(
          executeQuery(datasetId, query)
            .then((result) => ({ type: "chart", id: chart.id, result }))
            .catch((error) => {
              console.error(`❌ Chart ${chart.id} error:`, error);
              
              // If LLM query failed, try a simple fallback
              if ((chart as any).sql_query && (error.message.includes('syntax error') || error.message.includes('Binder Error') || error.message.includes('GROUP BY') || error.message.includes('not found in FROM clause'))) {
                console.log(`🔄 Attempting fallback query for Chart ${chart.id}`);
                
                // Generate simple fallback chart query
                let fallbackQuery = '';
                if (chart.x_axis && chart.y_axis) {
                  fallbackQuery = `SELECT ${chart.x_axis}, COUNT(*) as count FROM dataset WHERE ${chart.x_axis} IS NOT NULL GROUP BY ${chart.x_axis} ORDER BY count DESC LIMIT 10`;
                } else {
                  fallbackQuery = `SELECT 'No Data' as category, 0 as count FROM dataset LIMIT 1`;
                }
                
                // Apply filters to fallback query
                const whereClause = buildWhereClause(filters);
                if (whereClause && chart.x_axis) {
                  fallbackQuery = fallbackQuery.replace('WHERE ', `WHERE ${whereClause} AND `);
                }
                
                console.log(`📊 Chart ${chart.id} fallback query:`, fallbackQuery);
                
                return executeQuery(datasetId, fallbackQuery)
                  .then((result) => {
                    console.log(`✅ Chart ${chart.id} fallback result:`, result);
                    return { type: "chart", id: chart.id, result };
                  })
                  .catch((fallbackError) => {
                    console.error(`❌ Chart ${chart.id} fallback also failed:`, fallbackError);
                    return {
                      type: "chart",
                      id: chart.id,
                      error: `LLM query failed: ${error.message}. Fallback failed: ${fallbackError.message}`,
                    };
                  });
              }
              
              return {
                type: "chart",
                id: chart.id,
                error: error.message,
              };
            })
        );
      });

      // Execute all queries
      const results = await Promise.all(promises);

      // Process results
      const newKpiData: Record<string, QueryResult> = {};
      const newChartData: Record<string, QueryResult> = {};

      results.forEach((result) => {
        if (result.error) {
          console.error(
            `Error loading ${result.type} ${result.id}:`,
            result.error
          );
          onError?.(`Failed to load ${result.type}: ${result.error}`);
          return;
        }

        if (result.type === "kpi") {
          newKpiData[result.id] = result.result;
        } else if (result.type === "chart") {
          newChartData[result.id] = result.result;
        }
      });

      setKpiData(newKpiData);
      setChartData(newChartData);
    } catch (error: any) {
      console.error("Error loading dashboard data:", error);
      onError?.(error.message || "Failed to load dashboard data");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };



  // Inject filters into existing SQL query
  const injectFiltersIntoSQL = (originalQuery: string, whereClause: string): string => {
    const query = originalQuery.trim();
    
    // If query already has WHERE clause, combine with AND
    if (query.toUpperCase().includes(' WHERE ')) {
      // For complex queries with WHERE + GROUP BY/ORDER BY/LIMIT, we need to be smarter
      const whereIndex = query.toUpperCase().indexOf(' WHERE ');
      const beforeWhere = query.substring(0, whereIndex);
      
      // Find the end of the WHERE clause (before GROUP BY, ORDER BY, or LIMIT)
      let whereClauseEnd = query.length;
      const afterWhereText = query.substring(whereIndex + 7);
      
      // Find where the WHERE clause ends
      const groupByMatch = afterWhereText.toUpperCase().match(/^(.*?)\s+(GROUP\s+BY\s+.*)$/i);
      const orderByMatch = afterWhereText.toUpperCase().match(/^(.*?)\s+(ORDER\s+BY\s+.*)$/i);
      const limitMatch = afterWhereText.toUpperCase().match(/^(.*?)\s+(LIMIT\s+.*)$/i);
      
      let whereCondition = afterWhereText;
      let restOfQuery = '';
      
      if (groupByMatch) {
        whereCondition = groupByMatch[1];
        restOfQuery = ` ${groupByMatch[2]}`;
      } else if (orderByMatch) {
        whereCondition = orderByMatch[1];
        restOfQuery = ` ${orderByMatch[2]}`;
      } else if (limitMatch) {
        whereCondition = limitMatch[1];
        restOfQuery = ` ${limitMatch[2]}`;
      }
      
      return `${beforeWhere} WHERE (${whereCondition.trim()}) AND (${whereClause})${restOfQuery}`;
    }
    
    // For queries without WHERE, find insertion point before GROUP BY, ORDER BY, or LIMIT
    let insertionPoint = query.length;
    
    // Check for GROUP BY first (comes before ORDER BY and LIMIT)
    const groupByMatch = query.toUpperCase().match(/\s+(GROUP\s+BY\s+.*)$/i);
    if (groupByMatch) {
      insertionPoint = query.length - groupByMatch[1].length - 1;
      return `${query.substring(0, insertionPoint)} WHERE ${whereClause} ${groupByMatch[1]}`;
    }
    
    // Check for ORDER BY (comes before LIMIT)
    const orderByMatch = query.toUpperCase().match(/\s+(ORDER\s+BY\s+.*)$/i);
    if (orderByMatch) {
      insertionPoint = query.length - orderByMatch[1].length - 1;
      return `${query.substring(0, insertionPoint)} WHERE ${whereClause} ${orderByMatch[1]}`;
    }
    
    // Check for LIMIT (comes last)
    const limitMatch = query.toUpperCase().match(/\s+(LIMIT\s+.*)$/i);
    if (limitMatch) {
      insertionPoint = query.length - limitMatch[1].length - 1;
      return `${query.substring(0, insertionPoint)} WHERE ${whereClause} ${limitMatch[1]}`;
    }
    
    // No special clauses, just append WHERE at the end
    return `${query} WHERE ${whereClause}`;
  };

  // Apply filters using a robust CTE approach that rewrites queries to read from
  // a filtered view of the dataset. This safely handles subqueries and joins.
  const applyFiltersViaCTE = (originalQuery: string, whereClause: string): string => {
    const query = originalQuery.trim();
    if (!whereClause) return query;
    // Only proceed if the base table 'dataset' is present
    if (!/\bdataset\b/i.test(query)) return query;

    const filteredCTE = `filtered_dataset AS (SELECT * FROM dataset WHERE ${whereClause})`;
    let rewritten = query;

    // If the query already has a WITH clause, append our CTE at the start
    if (/^\s*WITH\s/i.test(query)) {
      rewritten = rewritten.replace(/^\s*WITH\s/i, `WITH ${filteredCTE}, `);
    } else {
      // Otherwise, prepend a new WITH clause
      rewritten = `WITH ${filteredCTE} ${query}`;
    }

    // Replace FROM/JOIN references to the base table with the filtered CTE
    // FROM dataset [AS alias]
    rewritten = rewritten.replace(/(\bFROM\s+)(dataset)(\s+AS\s+\w+|\s+\w+)?\b/gi, (_m, p1, _p2, p3 = "") => `${p1}filtered_dataset${p3}`);
    // JOIN dataset [AS alias]
    rewritten = rewritten.replace(/(\bJOIN\s+)(dataset)(\s+AS\s+\w+|\s+\w+)?\b/gi, (_m, p1, _p2, p3 = "") => `${p1}filtered_dataset${p3}`);
    // Comma separated table lists: ", dataset [AS alias]"
    rewritten = rewritten.replace(/,\s*dataset(\s+AS\s+\w+|\s+\w+)?\b/gi, (_m, p1 = "") => `, filtered_dataset${p1}`);

    return rewritten;
  };

  // Apply filters by wrapping every reference to the base table `dataset` with a filtered derived table.
  // This keeps the query starting with SELECT (passes backend safety) and works with subqueries.
  const applyFiltersToDatasetReferences = (originalQuery: string, whereClause: string): string => {
    const query = originalQuery.trim();
    if (!whereClause) return query;
    if (!/\bdataset\b/i.test(query)) return query;

    // Helper to produce a replacement string with optional alias
    const filteredDerived = (alias: string) => `(SELECT * FROM dataset WHERE ${whereClause}) AS ${alias}`;
    const reservedRegex = '(WHERE|GROUP|ORDER|LIMIT|HAVING|WINDOW|UNION|INTERSECT|EXCEPT|OFFSET|FETCH|JOIN|LEFT|RIGHT|FULL|INNER|OUTER|ON|USING|AND|OR)';

    let rewritten = query;

    // FROM dataset AS alias
    rewritten = rewritten.replace(/\bFROM\s+dataset\s+AS\s+(\w+)\b/gi, (_m, alias: string) => `FROM ${filteredDerived(alias)}`);
    // FROM dataset alias (avoid consuming WHERE/GROUP/...)
    rewritten = rewritten.replace(new RegExp(`\\bFROM\\s+dataset\\s+(?!${reservedRegex}\\b)(\\w+)\\b`, 'gi'), (_m, alias: string) => `FROM ${filteredDerived(alias)}`);
    // FROM dataset (no alias)
    rewritten = rewritten.replace(/\bFROM\s+dataset\b/gi, () => `FROM ${filteredDerived('dataset')}`);

    // JOIN dataset AS alias
    rewritten = rewritten.replace(/\bJOIN\s+dataset\s+AS\s+(\w+)\b/gi, (_m, alias: string) => `JOIN ${filteredDerived(alias)}`);
    // JOIN dataset alias (avoid reserved)
    rewritten = rewritten.replace(new RegExp(`\\bJOIN\\s+dataset\\s+(?!${reservedRegex}\\b)(\\w+)\\b`, 'gi'), (_m, alias: string) => `JOIN ${filteredDerived(alias)}`);
    // JOIN dataset (no alias)
    rewritten = rewritten.replace(/\bJOIN\s+dataset\b/gi, () => `JOIN ${filteredDerived('dataset')}`);

    // Comma-style lists
    // , dataset AS alias
    rewritten = rewritten.replace(/,\s*dataset\s+AS\s+(\w+)\b/gi, (_m, alias: string) => `, ${filteredDerived(alias)}`);
    // , dataset alias (avoid reserved)
    rewritten = rewritten.replace(new RegExp(`,\\s*dataset\\s+(?!${reservedRegex}\\b)(\\w+)\\b`, 'gi'), (_m, alias: string) => `, ${filteredDerived(alias)}`);
    // , dataset (no alias)
    rewritten = rewritten.replace(/,\s*dataset\b/gi, () => `, ${filteredDerived('dataset')}`);

    return rewritten;
  };

  // Build WHERE clause from active filters
  const buildWhereClause = (activeFilters: FilterState): string => {
    const conditions: string[] = [];

    Object.entries(activeFilters).forEach(([filterId, value]) => {
      const filter = config.filters.find((f) => f.id === filterId);
      if (!filter || value === undefined || value === null) return;

      switch (filter.type) {
        case "categorical":
        case "multi_select":
          if (Array.isArray(value) && value.length > 0) {
            const values = value.map((v) => `'${String(v).replace(/'/g, "''")}'`).join(", ");
            conditions.push(`${filter.column} IN (${values})`);
          } else if (typeof value === "string" && value) {
            conditions.push(`${filter.column} = '${value.replace(/'/g, "''")}'`);
          }
          break;
        case "numeric_range":
          if (typeof value === "object") {
            if (value.min !== undefined && value.min !== null && value.min !== "") {
              const minVal = parseFloat(value.min);
              if (!isNaN(minVal)) {
                conditions.push(`${filter.column} >= ${minVal}`);
              }
            }
            if (value.max !== undefined && value.max !== null && value.max !== "") {
              const maxVal = parseFloat(value.max);
              if (!isNaN(maxVal)) {
                conditions.push(`${filter.column} <= ${maxVal}`);
              }
            }
          }
          break;
        case "date_range":
          if (typeof value === "object" && value.start && value.end) {
            conditions.push(`${filter.column} BETWEEN '${value.start}' AND '${value.end}'`);
          }
          break;
      }
    });

    return conditions.join(" AND ");
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
      [DomainType.ECOMMERCE]: "from-blue-500 to-purple-600",
      [DomainType.FINANCE]: "from-green-500 to-teal-600",
      [DomainType.MANUFACTURING]: "from-orange-500 to-red-600",
      [DomainType.SAAS]: "from-cyan-500 to-blue-600",
      [DomainType.GENERIC]: "from-gray-500 to-gray-600",
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
                className={`text-white hover:bg-white/20 ${
                  showFilters ? "bg-white/20" : ""
                }`}
              >
                <Filter className="h-4 w-4 mr-2" />
                Filters
                {activeFilterCount > 0 && (
                  <span className="ml-2 bg-white/30 text-white text-xs px-2 py-0.5 rounded-full">
                    {activeFilterCount}
                  </span>
                )}
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
          <div className="bg-white rounded-lg border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Filters</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {config.filters.map((filter) => (
                <FilterComponent
                  key={filter.id}
                  filter={filter}
                  value={filters[filter.id]}
                  onChange={(value: any) => {
                    console.log(`Filter changed: ${filter.name} = ${JSON.stringify(value)}`);
                    setFilters((prev) => ({
                      ...prev,
                      [filter.id]: value,
                    }));
                  }}
                />
              ))}
            </div>
            <div className="mt-4 flex justify-end">
              <Button
                onClick={() => setFilters({})}
                variant="ghost"
                size="sm"
                className="text-gray-500 hover:text-gray-700"
              >
                Clear All Filters
              </Button>
            </div>
          </div>
        )}

        {/* KPIs */}
        {config.kpis.length > 0 && (
          <div className="mb-8">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              {config.kpis.map((kpi) => (
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
            {config.charts.map((chart) => (
              <div
                key={chart.id}
                className="bg-white border border-gray-200 rounded-lg p-6"
                style={{
                  gridColumn: chart.width > 6 ? "span 2" : "span 1",
                  minHeight: `${chart.height * 60}px`,
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
              <p className="text-lg font-medium">
                No dashboard components configured
              </p>
              <p className="text-sm mt-1">
                The dashboard configuration is empty or failed to load.
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

