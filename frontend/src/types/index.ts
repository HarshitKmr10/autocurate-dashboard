/**
 * TypeScript type definitions for the application
 */

// Processing status
export enum ProcessingStatus {
  PENDING = 'pending',
  PROCESSING = 'processing',
  COMPLETED = 'completed',
  FAILED = 'failed',
}

// Data types
export enum ColumnType {
  NUMERIC = 'numeric',
  CATEGORICAL = 'categorical',
  DATETIME = 'datetime',
  BOOLEAN = 'boolean',
  TEXT = 'text',
  MIXED = 'mixed',
}

export enum DomainType {
  ECOMMERCE = 'ecommerce',
  FINANCE = 'finance',
  MANUFACTURING = 'manufacturing',
  SAAS = 'saas',
  GENERIC = 'generic',
}

export enum ChartType {
  LINE = 'line',
  BAR = 'bar',
  PIE = 'pie',
  SCATTER = 'scatter',
  HISTOGRAM = 'histogram',
  HEATMAP = 'heatmap',
  FUNNEL = 'funnel',
  GAUGE = 'gauge',
  TABLE = 'table',
}

export enum FilterType {
  DATE_RANGE = 'date_range',
  CATEGORICAL = 'categorical',
  NUMERIC_RANGE = 'numeric_range',
  MULTI_SELECT = 'multi_select',
}

// Upload related types
export interface FileValidationResponse {
  is_valid: boolean;
  file_size: number;
  estimated_rows?: number;
  estimated_columns?: number;
  detected_delimiter?: string;
  detected_encoding?: string;
  error_message?: string;
  warnings: string[];
}

export interface UploadResponse {
  dataset_id: string;
  filename: string;
  file_size: number;
  status: ProcessingStatus;
  message: string;
  upload_time: string;
  estimated_completion_time?: string;
  processing_progress?: number;
}

export interface ProcessingStatusResponse {
  dataset_id: string;
  status: ProcessingStatus;
  message: string;
  progress?: number;
  created_at: string;
  updated_at: string;
  completion_time?: string;
  error_details?: string;
}

// Data profiling types
export interface ColumnProfile {
  name: string;
  original_name: string;
  data_type: ColumnType;
  null_count: number;
  null_percentage: number;
  unique_count: number;
  cardinality: number;
  min_value?: any;
  max_value?: any;
  mean_value?: number;
  median_value?: number;
  std_value?: number;
  min_length?: number;
  max_length?: number;
  avg_length?: number;
  sample_values: any[];
  top_values: Array<{
    value: string;
    count: number;
    percentage: number;
  }>;
  patterns: string[];
  is_id_like: boolean;
  is_email_like: boolean;
  is_phone_like: boolean;
  is_url_like: boolean;
}

export interface DataProfile {
  dataset_id: string;
  total_rows: number;
  total_columns: number;
  columns: ColumnProfile[];
  numeric_columns: string[];
  categorical_columns: string[];
  datetime_columns: string[];
  boolean_columns: string[];
  text_columns: string[];
  has_datetime: boolean;
  has_numeric: boolean;
  has_categorical: boolean;
  potential_target_columns: string[];
  potential_id_columns: string[];
  overall_null_percentage: number;
  high_cardinality_columns: string[];
  low_cardinality_columns: string[];
  correlations: Record<string, Record<string, number>>;
  profiled_at: string;
  sample_size: number;
}

// Domain classification types
export interface DomainClassification {
  domain: DomainType;
  confidence: number;
  reasoning: string;
  rule_based_score: number;
  llm_score: number;
  detected_patterns: string[];
  suggested_kpis: string[];
  classified_at: string;
}

// Dashboard configuration types
export interface KPIConfig {
  id: string;
  name: string;
  description: string;
  value_column: string;
  calculation: string;
  format_type: string;
  icon?: string;
  color?: string;
  trend_column?: string;
  importance: string;
  explanation: string;
}

export interface ChartConfig {
  id: string;
  type: ChartType;
  title: string;
  description: string;
  x_axis?: string;
  y_axis?: string;
  color_by?: string;
  size_by?: string;
  aggregation?: string;
  filters: string[];
  sort_by?: string;
  sort_order: string;
  limit?: number;
  width: number;
  height: number;
  importance: string;
  explanation: string;
}

export interface FilterConfig {
  id: string;
  name: string;
  column: string;
  type: FilterType;
  default_value?: any;
  options: any[];
  min_value?: any;
  max_value?: any;
  is_global: boolean;
}

export interface LayoutConfig {
  kpi_section: Record<string, any>;
  chart_section: Record<string, any>;
  filter_section: Record<string, any>;
  grid_columns: number;
  responsive_breakpoints: Record<string, number>;
}

export interface DashboardConfig {
  dataset_id: string;
  domain: string;
  title: string;
  description: string;
  kpis: KPIConfig[];
  charts: ChartConfig[];
  filters: FilterConfig[];
  layout: LayoutConfig;
  theme: string;
  refresh_interval?: number;
  created_at: string;
  explanation: string;
}

// API response types
export interface DashboardResponse {
  dataset_id: string;
  dashboard_config: DashboardConfig;
  domain_info: DomainClassification;
  profile: DataProfile;
  generated_at: string;
}

export interface DashboardPreviewResponse extends DashboardResponse {
  sample_data: {
    columns: string[];
    data: Record<string, any>[];
    row_count: number;
    query: string;
  };
  preview_limit: number;
}

export interface QueryResult {
  columns: string[];
  data: Record<string, any>[];
  row_count: number;
  query: string;
}

// Natural language query types
export interface NaturalLanguageQuery {
  intent: string;
  chart_type: ChartType;
  chart_config: {
    title: string;
    x_axis?: string;
    y_axis?: string;
    color_by?: string;
    aggregation?: string;
    filters: Record<string, any>;
  };
  execution_steps: string[];
  confidence: number;
  reasoning: string;
}

// UI State types
export interface FilterState {
  [filterId: string]: any;
}

export interface DashboardState {
  dataset_id?: string;
  dashboard_config?: DashboardConfig;
  profile?: DataProfile;
  domain_info?: DomainClassification;
  sample_data?: QueryResult;
  filters: FilterState;
  loading: boolean;
  error?: string;
}

export interface UploadState {
  uploading: boolean;
  processing: boolean;
  progress: number;
  error?: string;
  dataset_id?: string;
  status?: ProcessingStatus;
}

// Chart data types
export interface ChartData {
  [key: string]: any;
}

export interface PlotlyData {
  x?: any[];
  y?: any[];
  type: string;
  mode?: string;
  name?: string;
  marker?: any;
  line?: any;
  fill?: string;
  [key: string]: any;
}

export interface PlotlyLayout {
  title?: string;
  xaxis?: any;
  yaxis?: any;
  showlegend?: boolean;
  margin?: any;
  [key: string]: any;
}