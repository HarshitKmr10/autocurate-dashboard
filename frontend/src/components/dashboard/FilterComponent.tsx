"use client";

import React, { useState } from "react";
import { Calendar, ChevronDown } from "lucide-react";

interface FilterConfig {
  id: string;
  name: string;
  column: string;
  type: "categorical" | "date_range" | "numeric_range" | "multi_select";
  default_value?: any;
  options?: any[];
  min_value?: any;
  max_value?: any;
  is_global?: boolean;
}

interface FilterComponentProps {
  filter: FilterConfig;
  value: any;
  onChange: (value: any) => void;
}

export function FilterComponent({
  filter,
  value,
  onChange,
}: FilterComponentProps) {
  const [isOpen, setIsOpen] = useState(false);

  const renderCategoricalFilter = () => (
    <div className="relative">
      <label className="block text-sm font-medium text-gray-700 mb-2">
        {filter.name}
      </label>
      <select
        value={value || ""}
        onChange={(e) => onChange(e.target.value || null)}
        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
      >
        <option value="">All {filter.name}</option>
        {filter.options?.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    </div>
  );

  const renderMultiSelectFilter = () => {
    const selectedValues = Array.isArray(value) ? value : [];

    return (
      <div className="relative">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          {filter.name}
        </label>
        <div className="relative">
          <button
            type="button"
            onClick={() => setIsOpen(!isOpen)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-left focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 flex items-center justify-between"
          >
            <span className="text-sm text-gray-700">
              {selectedValues.length > 0
                ? `${selectedValues.length} selected`
                : `Select ${filter.name}`}
            </span>
            <ChevronDown className="h-4 w-4 text-gray-400" />
          </button>

          {isOpen && (
            <div className="absolute z-10 mt-1 w-full bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-auto">
              {filter.options?.map((option) => (
                <label
                  key={option}
                  className="flex items-center px-3 py-2 hover:bg-gray-50 cursor-pointer"
                >
                  <input
                    type="checkbox"
                    checked={selectedValues.includes(option)}
                    onChange={(e) => {
                      const newValues = e.target.checked
                        ? [...selectedValues, option]
                        : selectedValues.filter((v) => v !== option);
                      onChange(newValues.length > 0 ? newValues : null);
                    }}
                    className="mr-2 h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                  />
                  <span className="text-sm text-gray-700">{option}</span>
                </label>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  };

  const renderDateRangeFilter = () => {
    const dateValue = value || {};

    return (
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          {filter.name}
        </label>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <input
              type="date"
              value={dateValue.start || ""}
              onChange={(e) =>
                onChange({
                  ...dateValue,
                  start: e.target.value || null,
                })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm"
              placeholder="Start date"
            />
          </div>
          <div>
            <input
              type="date"
              value={dateValue.end || ""}
              onChange={(e) =>
                onChange({
                  ...dateValue,
                  end: e.target.value || null,
                })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm"
              placeholder="End date"
            />
          </div>
        </div>
        {/* Quick date range buttons */}
        <div className="mt-2 flex gap-1 flex-wrap">
          {[
            { label: "Last 7 days", days: 7 },
            { label: "Last 30 days", days: 30 },
            { label: "Last 90 days", days: 90 },
          ].map((preset) => (
            <button
              key={preset.days}
              onClick={() => {
                const end = new Date();
                const start = new Date();
                start.setDate(end.getDate() - preset.days);
                onChange({
                  start: start.toISOString().split("T")[0],
                  end: end.toISOString().split("T")[0],
                });
              }}
              className="px-2 py-1 text-xs bg-gray-100 hover:bg-gray-200 rounded text-gray-600 transition-colors"
            >
              {preset.label}
            </button>
          ))}
        </div>
      </div>
    );
  };

  const renderNumericRangeFilter = () => {
    const numericValue = value || {};

    return (
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          {filter.name}
        </label>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <input
              type="number"
              value={numericValue.min || ""}
              onChange={(e) =>
                onChange({
                  ...numericValue,
                  min: e.target.value ? parseFloat(e.target.value) : null,
                })
              }
              placeholder={`Min (${filter.min_value || 0})`}
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm"
            />
          </div>
          <div>
            <input
              type="number"
              value={numericValue.max || ""}
              onChange={(e) =>
                onChange({
                  ...numericValue,
                  max: e.target.value ? parseFloat(e.target.value) : null,
                })
              }
              placeholder={`Max (${filter.max_value || "∞"})`}
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm"
            />
          </div>
        </div>
      </div>
    );
  };

  // Close dropdown when clicking outside
  React.useEffect(() => {
    if (isOpen) {
      const handleClickOutside = () => setIsOpen(false);
      document.addEventListener("click", handleClickOutside);
      return () => document.removeEventListener("click", handleClickOutside);
    }
  }, [isOpen]);

  return (
    <div className="relative">
      {filter.type === "categorical" && renderCategoricalFilter()}
      {filter.type === "multi_select" && renderMultiSelectFilter()}
      {filter.type === "date_range" && renderDateRangeFilter()}
      {filter.type === "numeric_range" && renderNumericRangeFilter()}
    </div>
  );
}
