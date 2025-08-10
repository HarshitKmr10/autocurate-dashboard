"""Dashboard curation engine for generating dynamic dashboard configurations."""

from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from pydantic import BaseModel
from enum import Enum

from backend.core.profiler.data_profiler import DataProfile
from backend.core.domain.detector import DomainClassification
from backend.core.llm.client import LLMClient
from backend.utils.exceptions import DataProcessingException

logger = logging.getLogger(__name__)


class ChartType(str, Enum):
    """Enumeration of supported chart types."""
    LINE = "line"
    BAR = "bar"
    PIE = "pie"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"
    HEATMAP = "heatmap"
    FUNNEL = "funnel"
    GAUGE = "gauge"
    TABLE = "table"


class FilterType(str, Enum):
    """Enumeration of filter types."""
    DATE_RANGE = "date_range"
    CATEGORICAL = "categorical"
    NUMERIC_RANGE = "numeric_range"
    MULTI_SELECT = "multi_select"


class KPIConfig(BaseModel):
    """Configuration for a KPI card."""
    id: str
    name: str
    description: str
    value_column: str
    calculation: str  # sum, avg, count, max, min, etc.
    format_type: str  # currency, percentage, number, etc.
    icon: Optional[str] = None
    color: Optional[str] = None
    trend_column: Optional[str] = None
    importance: str = "medium"  # high, medium, low
    explanation: str = ""


class ChartConfig(BaseModel):
    """Configuration for a chart."""
    id: str
    type: ChartType
    title: str
    description: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    color_by: Optional[str] = None
    size_by: Optional[str] = None
    aggregation: Optional[str] = None
    filters: List[str] = []
    sort_by: Optional[str] = None
    sort_order: str = "desc"  # asc, desc
    limit: Optional[int] = None
    width: int = 6  # Grid width (1-12)
    height: int = 4  # Grid height
    importance: str = "medium"
    explanation: str = ""


class FilterConfig(BaseModel):
    """Configuration for a filter."""
    id: str
    name: str
    column: str
    type: FilterType
    default_value: Any = None
    options: List[Any] = []
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    is_global: bool = True


class LayoutConfig(BaseModel):
    """Configuration for dashboard layout."""
    kpi_section: Dict[str, Any]
    chart_section: Dict[str, Any]
    filter_section: Dict[str, Any]
    grid_columns: int = 12
    responsive_breakpoints: Dict[str, int] = {
        "xs": 576, "sm": 768, "md": 992, "lg": 1200, "xl": 1400
    }


class DashboardConfig(BaseModel):
    """Complete dashboard configuration."""
    dataset_id: str
    domain: str
    title: str
    description: str
    kpis: List[KPIConfig]
    charts: List[ChartConfig]
    filters: List[FilterConfig]
    layout: LayoutConfig
    theme: str = "light"
    refresh_interval: Optional[int] = None  # seconds
    created_at: datetime
    explanation: str = ""


class DashboardCurator:
    """Main dashboard curation engine."""
    
    def __init__(self):
        self.llm_client = LLMClient()
        
        # Default color schemes for different domains
        self.domain_colors = {
            "ecommerce": ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"],
            "finance": ["#2E8B57", "#4682B4", "#DAA520", "#CD853F", "#8B4513"],
            "manufacturing": ["#FF6347", "#4169E1", "#32CD32", "#FFD700", "#8A2BE2"],
            "saas": ["#00CED1", "#FF69B4", "#98FB98", "#F0E68C", "#DDA0DD"],
            "generic": ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
        }
    
    async def generate_dashboard(
        self,
        profile: DataProfile,
        domain_info: DomainClassification
    ) -> DashboardConfig:
        """
        Generate a complete dashboard configuration.
        
        Args:
            profile: Data profile from the profiler
            domain_info: Domain classification results
            
        Returns:
            Complete dashboard configuration
        """
        logger.info(f"Starting dashboard generation for {domain_info.domain} domain")
        
        try:
            # Prepare data summary for LLM
            profile_summary = self._prepare_profile_summary(profile)
            
            # Generate KPIs
            kpis = await self._generate_kpis(domain_info.domain, profile_summary, profile)
            
            # Generate charts
            charts = await self._generate_charts(domain_info.domain, profile_summary, kpis, profile)
            
            # Generate filters
            filters = await self._generate_filters(profile)
            
            # Generate layout
            layout = await self._generate_layout(kpis, charts, filters)
            
            # Create dashboard title and description
            title, description = self._generate_title_description(domain_info, profile)
            
            # Create complete configuration
            dashboard_config = DashboardConfig(
                dataset_id=profile.dataset_id,
                domain=domain_info.domain,
                title=title,
                description=description,
                kpis=kpis,
                charts=charts,
                filters=filters,
                layout=layout,
                created_at=datetime.utcnow(),
                explanation=f"Dashboard auto-generated for {domain_info.domain} domain with {domain_info.confidence:.0%} confidence"
            )
            
            logger.info(f"Dashboard generation completed: {len(kpis)} KPIs, {len(charts)} charts, {len(filters)} filters")
            return dashboard_config
            
        except Exception as e:
            logger.error(f"Dashboard generation failed: {e}", exc_info=True)
            raise DataProcessingException(f"Failed to generate dashboard: {str(e)}")
    
    def _prepare_profile_summary(self, profile: DataProfile) -> Dict[str, Any]:
        """Prepare a summary of the data profile for LLM analysis."""
        return {
            'total_rows': profile.total_rows,
            'total_columns': profile.total_columns,
            'columns': [
                {
                    'name': col.name,
                    'original_name': col.original_name,
                    'type': col.data_type,
                    'sample_values': col.sample_values[:5],
                    'unique_count': col.unique_count,
                    'null_percentage': col.null_percentage,
                    'top_values': col.top_values[:3] if col.top_values else []
                }
                for col in profile.columns[:20]  # Limit to first 20 columns
            ],
            'numeric_columns': profile.numeric_columns,
            'categorical_columns': profile.categorical_columns,
            'datetime_columns': profile.datetime_columns,
            'has_datetime': profile.has_datetime,
            'has_numeric': profile.has_numeric,
            'potential_id_columns': profile.potential_id_columns
        }
    
    async def _generate_kpis(
        self,
        domain: str,
        profile_summary: Dict[str, Any],
        profile: DataProfile
    ) -> List[KPIConfig]:
        """Generate KPI configurations using LLM."""
        try:
            # Get LLM suggestions for KPIs
            llm_result = await self.llm_client.select_kpis(domain, profile_summary)
            
            kpis = []
            for i, kpi_data in enumerate(llm_result.get('selected_kpis', [])[:3]):
                kpi_config = KPIConfig(
                    id=f"kpi_{i+1}",
                    name=kpi_data.get('name', f'KPI {i+1}'),
                    description=kpi_data.get('description', ''),
                    value_column=self._find_best_column_match(
                        kpi_data.get('columns_needed', []), 
                        profile
                    ),
                    calculation=kpi_data.get('calculation', '').lower().split(' ')[0] or 'sum',
                    format_type=self._infer_format_type(kpi_data.get('name', ''), domain),
                    color=self.domain_colors[domain][i % len(self.domain_colors[domain])],
                    importance=kpi_data.get('importance', 'medium'),
                    explanation=kpi_data.get('reasoning', '')
                )
                kpis.append(kpi_config)
            
            # If LLM didn't provide enough KPIs, add fallback ones
            if len(kpis) == 0:
                kpis = self._generate_fallback_kpis(profile, domain)
            
            return kpis
            
        except Exception as e:
            logger.warning(f"LLM KPI generation failed: {e}. Using fallback KPIs.")
            return self._generate_fallback_kpis(profile, domain)
    
    async def _generate_charts(
        self,
        domain: str,
        profile_summary: Dict[str, Any],
        kpis: List[KPIConfig],
        profile: DataProfile
    ) -> List[ChartConfig]:
        """Generate chart configurations using LLM."""
        try:
            # Prepare KPI data for LLM
            kpi_data = [
                {
                    'name': kpi.name,
                    'description': kpi.description,
                    'column': kpi.value_column
                }
                for kpi in kpis
            ]
            
            # Get LLM suggestions for charts
            llm_result = await self.llm_client.select_charts(domain, profile_summary, kpi_data)
            
            charts = []
            for i, chart_data in enumerate(llm_result.get('selected_charts', [])[:6]):
                chart_config = ChartConfig(
                    id=f"chart_{i+1}",
                    type=ChartType(chart_data.get('type', 'bar')),
                    title=chart_data.get('title', f'Chart {i+1}'),
                    description=chart_data.get('description', ''),
                    x_axis=self._find_best_column_match([chart_data.get('x_axis')], profile) if chart_data.get('x_axis') else None,
                    y_axis=self._find_best_column_match([chart_data.get('y_axis')], profile) if chart_data.get('y_axis') else None,
                    color_by=self._find_best_column_match([chart_data.get('color_by')], profile) if chart_data.get('color_by') else None,
                    aggregation=chart_data.get('aggregation'),
                    width=6 if i < 2 else 4,  # First two charts are wider
                    height=4,
                    importance=chart_data.get('importance', 'medium'),
                    explanation=chart_data.get('reasoning', '')
                )
                charts.append(chart_config)
            
            # If LLM didn't provide charts, add fallback ones
            if len(charts) == 0:
                charts = self._generate_fallback_charts(profile)
            
            return charts
            
        except Exception as e:
            logger.warning(f"LLM chart generation failed: {e}. Using fallback charts.")
            return self._generate_fallback_charts(profile)
    
    async def _generate_filters(self, profile: DataProfile) -> List[FilterConfig]:
        """Generate filter configurations based on data profile."""
        filters = []
        
        # Add date filter if datetime columns exist
        if profile.datetime_columns:
            date_col = profile.datetime_columns[0]
            filters.append(FilterConfig(
                id="date_filter",
                name="Date Range",
                column=date_col,
                type=FilterType.DATE_RANGE,
                is_global=True
            ))
        
        # Add categorical filters for low-cardinality columns
        for col_name in profile.categorical_columns[:3]:  # Limit to 3 categorical filters
            col_profile = next((col for col in profile.columns if col.name == col_name), None)
            if col_profile and col_profile.unique_count <= 20:  # Low cardinality
                filters.append(FilterConfig(
                    id=f"filter_{col_name}",
                    name=col_profile.original_name,
                    column=col_name,
                    type=FilterType.MULTI_SELECT,
                    options=[v['value'] for v in col_profile.top_values] if col_profile.top_values else [],
                    is_global=True
                ))
        
        return filters
    
    async def _generate_layout(
        self,
        kpis: List[KPIConfig],
        charts: List[ChartConfig],
        filters: List[FilterConfig]
    ) -> LayoutConfig:
        """Generate layout configuration."""
        return LayoutConfig(
            kpi_section={
                "position": "top",
                "height": "auto",
                "columns": min(len(kpis), 4)
            },
            chart_section={
                "position": "middle",
                "grid_type": "responsive",
                "gap": 16
            },
            filter_section={
                "position": "sidebar",
                "width": 250,
                "collapsible": True
            }
        )
    
    def _generate_title_description(
        self,
        domain_info: DomainClassification,
        profile: DataProfile
    ) -> tuple[str, str]:
        """Generate dashboard title and description."""
        domain_titles = {
            "ecommerce": "E-commerce Analytics Dashboard",
            "finance": "Financial Analytics Dashboard", 
            "manufacturing": "Manufacturing Operations Dashboard",
            "saas": "SaaS Metrics Dashboard",
            "generic": "Data Analytics Dashboard"
        }
        
        title = domain_titles.get(domain_info.domain, "Analytics Dashboard")
        
        description = (f"Auto-generated dashboard for {domain_info.domain} data analysis. "
                      f"Analyzing {profile.total_rows:,} rows across {profile.total_columns} columns.")
        
        return title, description
    
    def _find_best_column_match(self, suggested_columns: List[str], profile: DataProfile) -> str:
        """Find the best matching column from suggestions."""
        if not suggested_columns:
            return ""
        
        # First try exact matches
        available_columns = [col.name for col in profile.columns]
        for suggested in suggested_columns:
            if suggested in available_columns:
                return suggested
        
        # Then try case-insensitive matches
        available_lower = {col.name.lower(): col.name for col in profile.columns}
        for suggested in suggested_columns:
            if suggested.lower() in available_lower:
                return available_lower[suggested.lower()]
        
        # Finally try partial matches
        for suggested in suggested_columns:
            for available in available_columns:
                if suggested.lower() in available.lower() or available.lower() in suggested.lower():
                    return available
        
        # Return first numeric column as fallback
        if profile.numeric_columns:
            return profile.numeric_columns[0]
        
        return ""
    
    def _infer_format_type(self, kpi_name: str, domain: str) -> str:
        """Infer the format type for a KPI based on its name and domain."""
        name_lower = kpi_name.lower()
        
        if any(word in name_lower for word in ['revenue', 'sales', 'cost', 'price', 'amount', '$']):
            return 'currency'
        elif any(word in name_lower for word in ['rate', 'percentage', '%', 'ratio']):
            return 'percentage'
        elif any(word in name_lower for word in ['count', 'number', 'quantity', 'total']):
            return 'number'
        else:
            return 'number'
    
    def _generate_fallback_kpis(self, profile: DataProfile, domain: str) -> List[KPIConfig]:
        """Generate fallback KPIs when LLM fails."""
        kpis = []
        
        # Total count KPI
        kpis.append(KPIConfig(
            id="kpi_count",
            name="Total Records",
            description="Total number of records in the dataset",
            value_column="*",
            calculation="count",
            format_type="number",
            color=self.domain_colors[domain][0],
            explanation="Basic count of all records in the dataset"
        ))
        
        # Numeric column KPI if available
        if profile.numeric_columns:
            col_name = profile.numeric_columns[0]
            kpis.append(KPIConfig(
                id="kpi_numeric",
                name=f"Total {col_name}",
                description=f"Sum of {col_name}",
                value_column=col_name,
                calculation="sum",
                format_type="number",
                color=self.domain_colors[domain][1],
                explanation=f"Sum of all values in {col_name}"
            ))
        
        return kpis
    
    def _generate_fallback_charts(self, profile: DataProfile) -> List[ChartConfig]:
        """Generate fallback charts when LLM fails."""
        charts = []
        
        # Bar chart for categorical data
        if profile.categorical_columns and profile.numeric_columns:
            charts.append(ChartConfig(
                id="chart_bar",
                type=ChartType.BAR,
                title=f"{profile.categorical_columns[0]} Distribution",
                description=f"Distribution of {profile.categorical_columns[0]}",
                x_axis=profile.categorical_columns[0],
                y_axis=profile.numeric_columns[0],
                aggregation="sum",
                width=6,
                height=4
            ))
        
        # Time series chart if datetime available
        if profile.datetime_columns and profile.numeric_columns:
            charts.append(ChartConfig(
                id="chart_timeseries",
                type=ChartType.LINE,
                title="Time Series Analysis",
                description="Trend over time",
                x_axis=profile.datetime_columns[0],
                y_axis=profile.numeric_columns[0],
                aggregation="sum",
                width=6,
                height=4
            ))
        
        return charts