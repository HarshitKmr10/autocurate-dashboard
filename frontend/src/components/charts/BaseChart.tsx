/**
 * Base chart component that handles different chart types
 */

"use client";

import React, { useMemo } from "react";
import dynamic from "next/dynamic";
import { ChartType, ChartConfig, QueryResult } from "@/types";
import { generateColors } from "@/lib/utils";

// Dynamically import Plotly to avoid SSR issues
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

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
  Scatter,
} from "recharts";

interface BaseChartProps {
  config: ChartConfig;
  data: QueryResult;
  width?: number;
  height?: number;
  onError?: (error: string) => void;
}

interface DataValidationResult {
  isValid: boolean;
  error?: string;
  warning?: string;
  processedData?: any[];
}

export function BaseChart({
  config,
  data,
  width = 400,
  height = 300,
  onError,
}: BaseChartProps) {
  const dataValidation = useMemo(() => {
    return validateChartData(data, config);
  }, [data, config]);

  const chartData = useMemo(() => {
    if (!dataValidation.isValid) {
      return [];
    }

    return dataValidation.processedData || [];
  }, [dataValidation]);

  const colors = useMemo(
    () => generateColors(chartData.length),
    [chartData.length]
  );

  // If data validation failed, show error state
  if (!dataValidation.isValid) {
    return (
      <div className="bg-white p-6 rounded-lg shadow border">
        <h3 className="text-lg font-semibold text-gray-900 mb-2">
          {config.title}
        </h3>
        {config.description && (
          <p className="text-sm text-gray-600 mb-4">{config.description}</p>
        )}
        <div className="flex items-center justify-center h-64 bg-gray-50 rounded border-2 border-dashed border-gray-300">
          <div className="text-center">
            <div className="text-gray-400 mb-2">
              <svg
                className="mx-auto h-12 w-12"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 00-2-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
            </div>
            <p className="text-gray-500 font-medium">Chart Unavailable</p>
            <p className="text-sm text-gray-400 mt-1">
              {dataValidation.error ||
                "Insufficient data for meaningful visualization"}
            </p>
          </div>
        </div>
      </div>
    );
  }

  const renderRechartsChart = () => {
    try {
      const commonProps = {
        data: chartData,
        margin: { top: 20, right: 30, left: 20, bottom: 20 },
      };

      // If no data, render empty state
      if (!chartData || chartData.length === 0) {
        return (
          <div className="flex items-center justify-center h-full text-gray-500">
            <div className="text-center">
              <div className="text-gray-400 mb-2">📊</div>
              <p>No data to display</p>
            </div>
          </div>
        );
      }

      console.log(`Rendering ${config.type} chart with data:`, chartData);

      switch (config.type) {
        case ChartType.PIE:
          return (
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={chartData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent, value }) => {
                    const formattedValue =
                      typeof value === "number"
                        ? value.toLocaleString()
                        : value;
                    return `${name}: ${formattedValue} (${(
                      (percent || 0) * 100
                    ).toFixed(1)}%)`;
                  }}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey={config.y_axis || "count"} // Fallback to 'count'
                  nameKey={config.x_axis || Object.keys(chartData[0] || {})[0]} // Fallback to first column
                >
                  {chartData.map((entry, index) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={colors[index % colors.length]}
                    />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(value: any, name: string) => {
                    if (typeof value === "number") {
                      return [value.toLocaleString(), name];
                    }
                    return [value, name];
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          );

        case ChartType.BAR:
          return (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart {...commonProps}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey={config.x_axis || Object.keys(chartData[0] || {})[0]}
                  tick={{ fontSize: 12 }}
                  angle={-45}
                  textAnchor="end"
                  height={60}
                />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip
                  formatter={(value: any, name: string) => {
                    if (typeof value === "number") {
                      if (value >= 1000000) {
                        return [`${(value / 1000000).toFixed(1)}M`, name];
                      } else if (value >= 1000) {
                        return [`${(value / 1000).toFixed(1)}K`, name];
                      }
                      return [value.toLocaleString(), name];
                    }
                    return [value, name];
                  }}
                />
                <Legend />
                <Bar
                  dataKey={config.y_axis || "count"} // Fallback to 'count'
                  fill={colors[0]}
                  name={config.y_axis || "Count"}
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
                  dataKey={config.x_axis || Object.keys(chartData[0] || {})[0]}
                  tick={{ fontSize: 12 }}
                />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip
                  formatter={(value: any, name: string) => {
                    if (typeof value === "number") {
                      if (value >= 1000000) {
                        return [`${(value / 1000000).toFixed(1)}M`, name];
                      } else if (value >= 1000) {
                        return [`${(value / 1000).toFixed(1)}K`, name];
                      }
                      return [value.toLocaleString(), name];
                    }
                    return [value, name];
                  }}
                />
                <Legend />
                <Line
                  type="monotone"
                  dataKey={config.y_axis || "count"} // Fallback to 'count'
                  stroke={colors[0]}
                  strokeWidth={2}
                  dot={{ r: 4 }}
                  name={config.y_axis || "Count"}
                  connectNulls={false}
                />
              </LineChart>
            </ResponsiveContainer>
          );

        case ChartType.SCATTER:
          return (
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart {...commonProps}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  type="number"
                  dataKey={config.x_axis || Object.keys(chartData[0] || {})[0]}
                  tick={{ fontSize: 12 }}
                  name={config.x_axis}
                />
                <YAxis
                  type="number"
                  dataKey={config.y_axis || Object.keys(chartData[0] || {})[1]}
                  tick={{ fontSize: 12 }}
                  name={config.y_axis}
                />
                <Tooltip
                  cursor={{ strokeDasharray: "3 3" }}
                  formatter={(value: any, name: string) => {
                    if (typeof value === "number") {
                      return [value.toLocaleString(), name];
                    }
                    return [value, name];
                  }}
                />
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
              <div className="text-center">
                <div className="text-gray-400 mb-2">🚧</div>
                <p>Chart type not supported: {config.type}</p>
                <p className="text-sm">Try a different chart type</p>
              </div>
            </div>
          );
      }
    } catch (error) {
      console.error("Error rendering chart:", error);
      return (
        <div className="flex items-center justify-center h-full text-gray-500">
          <div className="text-center">
            <div className="text-gray-400 mb-2">⚠️</div>
            <p>Error rendering chart</p>
            <p className="text-sm text-red-500">{String(error)}</p>
          </div>
        </div>
      );
    }
  };

  const renderPlotlyChart = () => {
    try {
      const plotData: any[] = [];
      const layout: any = {
        title: {
          text: config.title,
          font: { size: 14 },
        },
        margin: { t: 50, r: 50, b: 50, l: 50 },
        showlegend: true,
        autosize: true,
      };

      switch (config.type) {
        case ChartType.HISTOGRAM:
          plotData.push({
            x: chartData.map((d) => d[config.x_axis!]),
            type: "histogram",
            marker: { color: colors[0] },
            name: config.x_axis,
          });
          break;

        case ChartType.HEATMAP:
          // This would need more complex data transformation for heatmaps
          plotData.push({
            z: chartData.map((d) => [d[config.x_axis!], d[config.y_axis!]]),
            type: "heatmap",
            colorscale: "Viridis",
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
          style={{ width: "100%", height: "100%" }}
          useResizeHandler={true}
          config={{
            displayModeBar: false,
            responsive: true,
          }}
        />
      );
    } catch (error) {
      console.error("Plotly chart error:", error);
      onError?.(`Failed to render chart: ${error}`);
      return null;
    }
  };

  // Determine which chart library to use
  const useplotly = [
    ChartType.HISTOGRAM,
    ChartType.HEATMAP,
    ChartType.FUNNEL,
    ChartType.GAUGE,
  ].includes(config.type);

  return (
    <div className="bg-white p-6 rounded-lg shadow border">
      <h3 className="text-lg font-semibold text-gray-900 mb-2">
        {config.title}
      </h3>
      {config.description && (
        <p className="text-sm text-gray-600 mb-4">{config.description}</p>
      )}

      {dataValidation.warning && (
        <div className="mb-4 p-3 bg-yellow-50 border border-yellow-200 rounded-md">
          <div className="flex">
            <div className="flex-shrink-0">
              <svg
                className="h-5 w-5 text-yellow-400"
                viewBox="0 0 20 20"
                fill="currentColor"
              >
                <path
                  fillRule="evenodd"
                  d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z"
                  clipRule="evenodd"
                />
              </svg>
            </div>
            <div className="ml-3">
              <p className="text-sm text-yellow-700">
                {dataValidation.warning}
              </p>
            </div>
          </div>
        </div>
      )}

      <div className="h-64">
        {useplotly ? renderPlotlyChart() : renderRechartsChart()}
      </div>
    </div>
  );
}

function validateChartData(
  data: QueryResult,
  config: ChartConfig
): DataValidationResult {
  try {
    // Debug: Log the incoming data structure
    console.log(
      "Chart validation - incoming data:",
      JSON.stringify(data, null, 2)
    );

    // Try multiple ways to extract the actual data array
    let rawData: any[] = [];

    // Case 1: Direct data array
    if (Array.isArray(data)) {
      rawData = data;
    }
    // Case 2: data.data array
    else if (data && Array.isArray((data as any).data)) {
      rawData = (data as any).data;
    }
    // Case 3: data.results.data array
    else if (
      data &&
      (data as any).results &&
      Array.isArray((data as any).results.data)
    ) {
      rawData = (data as any).results.data;
    }
    // Case 4: data.result.data array
    else if (
      data &&
      (data as any).result &&
      Array.isArray((data as any).result.data)
    ) {
      rawData = (data as any).result.data;
    }
    // Case 5: data.results array (direct)
    else if (data && Array.isArray((data as any).results)) {
      rawData = (data as any).results;
    } else {
      console.error("Could not extract data array from:", data);
      return {
        isValid: false,
        error: "Could not find data array in response",
      };
    }

    console.log("Extracted raw data:", rawData);

    // Very lenient check - even 0 rows can be valid for some charts
    if (!Array.isArray(rawData)) {
      return {
        isValid: false,
        error: "Data is not an array",
      };
    }

    // If no data, still try to render an empty chart
    if (rawData.length === 0) {
      return {
        isValid: true,
        processedData: [],
        warning: "No data available to display",
      };
    }

    // Very lenient column validation - try to work with whatever we have
    let processedData = rawData;

    // If we have config axes, try to validate them, but don't fail if they're missing
    if (config.x_axis || config.y_axis) {
      const cleanedData = cleanChartData(rawData, config);

      // Even if cleaned data is empty, still allow the chart to render
      processedData = cleanedData.length > 0 ? cleanedData : rawData;
    }

    // Super lenient chart-specific validation
    const chartValidation = validateChartSpecificLenient(processedData, config);

    // Always return valid, just with warnings if needed
    return {
      isValid: true,
      processedData: processedData,
      warning: chartValidation.warning,
    };
  } catch (error) {
    console.error("Chart validation error:", error);
    // Even on error, try to return something workable
    return {
      isValid: true,
      processedData: [],
      warning: "Error processing chart data - showing empty chart",
    };
  }
}

function hasColumn(data: any[], columnName: string): boolean {
  return data.length > 0 && columnName in data[0];
}

function cleanChartData(data: any[], config: ChartConfig): any[] {
  if (!config.x_axis) {
    return data; // Return all data if no x_axis configured
  }

  return data
    .filter((row) => {
      // Very lenient filtering - only remove truly empty/problematic rows
      const xValue = row[config.x_axis!];
      const yValue = config.y_axis ? row[config.y_axis] : true;

      // Only filter out completely invalid values
      const isXValid =
        xValue !== null && xValue !== undefined && String(xValue).trim() !== "";
      const isYValid =
        !config.y_axis ||
        (yValue !== null &&
          yValue !== undefined &&
          String(yValue).trim() !== "");

      return isXValid && isYValid;
    })
    .map((row) => {
      // Process the row to ensure proper data types for charts
      const processedRow = { ...row };

      // Try to convert numeric-looking strings to numbers for better chart rendering
      if (config.y_axis && typeof row[config.y_axis] === "string") {
        const numValue = parseFloat(row[config.y_axis]);
        if (!isNaN(numValue)) {
          processedRow[config.y_axis] = numValue;
        }
      }

      // Also try x_axis in case it should be numeric (for scatter plots, etc.)
      if (config.x_axis && typeof row[config.x_axis] === "string") {
        const numValue = parseFloat(row[config.x_axis]);
        if (!isNaN(numValue)) {
          processedRow[config.x_axis] = numValue;
        }
      }

      return processedRow;
    });
}

function validateChartSpecificLenient(
  data: any[],
  config: ChartConfig
): DataValidationResult {
  // Super lenient validation - always return valid, just with helpful warnings
  try {
    let warning = "";

    if (data.length === 0) {
      warning = "No data available to display";
    } else if (data.length === 1) {
      warning = "Only one data point available - chart may be limited";
    }

    switch (config.type) {
      case ChartType.PIE:
        if (!config.x_axis) {
          warning =
            "Pie chart may not display correctly without X-axis configuration";
        } else {
          const categories = new Set(data.map((row) => row[config.x_axis!]));
          if (categories.size > 20) {
            warning = "Many categories detected - chart may be crowded";
          }
        }
        break;

      case ChartType.SCATTER:
        if (!config.x_axis || !config.y_axis) {
          warning =
            "Scatter plot may not display correctly without both X and Y axes";
        }
        break;

      case ChartType.LINE:
        if (data.length > 100) {
          warning = "Large dataset detected - chart may be crowded";
        }
        break;
    }

    return {
      isValid: true,
      warning: warning || undefined,
    };
  } catch (error) {
    return {
      isValid: true,
      warning: "Chart configuration may have issues but will attempt to render",
    };
  }
}

function calculateNullPercentage(data: any[]): number {
  if (data.length === 0) return 0;

  const totalValues = data.length * Object.keys(data[0]).length;
  let nullCount = 0;

  data.forEach((row) => {
    Object.values(row).forEach((value) => {
      if (
        value === null ||
        value === undefined ||
        value === "" ||
        value === "null"
      ) {
        nullCount++;
      }
    });
  });

  return (nullCount / totalValues) * 100;
}
