"""Dashboard curation engine for generating dynamic dashboard configurations."""

from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from pydantic import BaseModel
from enum import Enum
import json
import asyncio

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
    sql_query: Optional[str] = None  # Added for custom SQL


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
    sql_query: Optional[str] = None  # Added for custom SQL


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
        "xs": 576,
        "sm": 768,
        "md": 992,
        "lg": 1200,
        "xl": 1400,
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
            "generic": ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"],
        }

    async def generate_dashboard(
        self, profile: DataProfile, domain_info: DomainClassification
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
            kpis = await self._generate_kpis(
                domain_info.domain, profile_summary, profile
            )

            # Generate charts
            charts = await self._generate_charts(
                domain_info.domain, profile_summary, kpis, profile
            )

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
                explanation=f"Dashboard auto-generated for {domain_info.domain} domain with {domain_info.confidence:.0%} confidence",
            )

            logger.info(
                f"Dashboard generation completed: {len(kpis)} KPIs, {len(charts)} charts, {len(filters)} filters"
            )
            return dashboard_config

        except Exception as e:
            logger.error(f"Dashboard generation failed: {e}", exc_info=True)
            raise DataProcessingException(f"Failed to generate dashboard: {str(e)}")

    def _prepare_profile_summary(self, profile: DataProfile) -> Dict[str, Any]:
        """Prepare a summary of the data profile for LLM analysis."""
        return {
            "total_rows": profile.total_rows,
            "total_columns": profile.total_columns,
            "columns": [
                {
                    "name": col.name,
                    "original_name": col.original_name,
                    "type": col.data_type,
                    "sample_values": col.sample_values[:5],
                    "unique_count": col.unique_count,
                    "null_percentage": col.null_percentage,
                    "top_values": col.top_values[:3] if col.top_values else [],
                }
                for col in profile.columns[:20]  # Limit to first 20 columns
            ],
            "numeric_columns": profile.numeric_columns,
            "categorical_columns": profile.categorical_columns,
            "datetime_columns": profile.datetime_columns,
            "has_datetime": profile.has_datetime,
            "has_numeric": profile.has_numeric,
            "potential_id_columns": profile.potential_id_columns,
        }

    async def _generate_kpis(
        self, domain: str, profile_summary: Dict[str, Any], profile: DataProfile
    ) -> List[KPIConfig]:
        """Generate KPI configurations using enhanced LLM calls with data context."""
        try:
            logger.info(
                f"Generating KPIs for {domain} domain with enhanced data context"
            )

            # Get sample data for context
            sample_data = self._get_sample_data_for_llm(profile_summary)

            # Generate each KPI individually with proper context
            kpis: List[KPIConfig] = []
            target_kpi_count = 3

            for i in range(target_kpi_count):
                try:
                    kpi_config = await self._generate_single_kpi(
                        domain=domain,
                        profile_summary=profile_summary,
                        sample_data=sample_data,
                        existing_kpis=[kpi.name for kpi in kpis],
                        kpi_index=i + 1,
                    )
                    if kpi_config:
                        kpis.append(kpi_config)
                        logger.info(f"Generated KPI {i+1}: {kpi_config.name}")
                except Exception as e:
                    logger.error(f"Failed to generate KPI {i+1}: {e}")
                    continue

            # If we don't have enough KPIs, add fallback ones
            if len(kpis) < 2:
                logger.warning("Not enough KPIs generated, adding fallback KPIs")
                fallback_kpis = self._generate_fallback_kpis(profile, domain)
                kpis.extend(fallback_kpis[: 3 - len(kpis)])

            return kpis[:3]  # Return max 3 KPIs

        except Exception as e:
            logger.error(f"Enhanced KPI generation failed: {e}. Using fallback KPIs.")
            return self._generate_fallback_kpis(profile, domain)

    def _get_sample_data_for_llm(self, profile_summary: Dict[str, Any]) -> str:
        """Extract meaningful sample data for LLM context with proper validation."""
        try:
            sample_rows = []

            # Get column information with samples and validate data
            columns = profile_summary.get("columns", [])
            if not columns:
                logger.warning("No columns found in profile summary")
                return "No column data available"

            # Limit to 10 most important columns for better LLM performance
            for i, col in enumerate(columns[:10]):
                try:
                    col_name = col.get("name", f"unknown_column_{i}")
                    col_type = col.get("type", "unknown")
                    sample_values = col.get("sample_values", [])

                    # Sanitize sample values
                    clean_samples = []
                    for val in sample_values[:3]:  # First 3 sample values
                        if val is not None and str(val).lower() not in [
                            "nan",
                            "null",
                            "",
                        ]:
                            # Truncate long strings for better LLM performance
                            clean_val = (
                                str(val)[:50] if len(str(val)) > 50 else str(val)
                            )
                            clean_samples.append(clean_val)

                    if not clean_samples:
                        clean_samples = ["[no valid samples]"]

                    sample_rows.append(
                        f"- {col_name} ({col_type}): {', '.join(clean_samples)}"
                    )

                except Exception as e:
                    logger.warning(f"Error processing column {i}: {e}")
                    continue

            if not sample_rows:
                return "No valid column samples available"

            result = "\n".join(sample_rows)
            logger.info(
                f"Generated sample data for LLM with {len(sample_rows)} columns"
            )
            return result

        except Exception as e:
            logger.error(f"Error generating sample data for LLM: {e}")
            return "Error extracting sample data"

    async def _generate_single_kpi(
        self,
        domain: str,
        profile_summary: Dict[str, Any],
        sample_data: str,
        existing_kpis: List[str],
        kpi_index: int,
    ) -> Optional[KPIConfig]:
        """Generate a single KPI with full data context and validation."""

        try:
            # Validate inputs
            if not domain or not profile_summary:
                logger.error("Invalid inputs for KPI generation")
                return None

            # Get numeric columns for validation
            numeric_columns = profile_summary.get("numeric_columns", [])
            categorical_columns = profile_summary.get("categorical_columns", [])

            if not numeric_columns and not categorical_columns:
                logger.warning("No suitable columns found for KPI generation")
                return None

            system_prompt = f"""You are a business intelligence expert specializing in {domain} analytics.
Your task is to design ONE meaningful KPI (Key Performance Indicator) that provides actionable business insights.

CRITICAL REQUIREMENTS:
1. Generate REAL business metrics, not just row counts
2. Use appropriate SQL calculations (SUM, AVG, ratios, percentages)
3. Choose columns that make business sense for the calculation
4. Ensure the KPI provides actionable insights for {domain} business
5. SQL query must return exactly ONE row with a column named 'value'

IMPORTANT: Use DuckDB SQL syntax (NOT MySQL/PostgreSQL):
- Date functions: CURRENT_DATE (not CURDATE())
- Date arithmetic: CURRENT_DATE - INTERVAL '30' DAY (not DATE_SUB)
- Date comparison: WHERE date_col >= CURRENT_DATE - INTERVAL '30' DAY
- Cast if needed: CAST(column AS DATE)

For {domain} domain, focus on metrics like:
- Revenue/financial performance (use amount/price columns with SUM/AVG)
- Operational efficiency (use status/completion data with percentages)
- Customer behavior (use customer-related metrics with COUNT/AVG)
- Growth indicators (use time-based trends)

STRICT SQL REQUIREMENTS:
- Query MUST return exactly one column named 'value'
- Use proper aggregation functions (SUM, AVG, COUNT, etc.)
- Handle potential NULL values with COALESCE or WHERE clauses
- Use DuckDB-compatible syntax only
- Example: "SELECT AVG(total_amount) as value FROM dataset WHERE total_amount > 0"

AVOID:
- Simple row counts unless specifically needed for business context
- Meaningless calculations
- Using non-numeric columns for SUM/AVG operations
- MySQL/PostgreSQL specific functions like DATE_SUB, CURDATE
- Complex queries that might fail"""

            user_prompt = f"""Dataset Overview:
- Domain: {domain}
- Total Rows: {profile_summary.get('total_rows', 0):,}
- Total Columns: {profile_summary.get('total_columns', 0)}

Available Columns with Sample Data:
{sample_data}

Numeric Columns (for calculations): {', '.join(numeric_columns[:5])}
Categorical Columns (for grouping/filtering): {', '.join(categorical_columns[:5])}
DateTime Columns: {', '.join(profile_summary.get('datetime_columns', [])[:3])}

Existing KPIs already created: {', '.join(existing_kpis) if existing_kpis else 'None'}

Please design KPI #{kpi_index} that is:
1. DIFFERENT from existing KPIs
2. MEANINGFUL for {domain} business decisions
3. Uses APPROPRIATE calculations for the data types
4. Returns a single numeric value

REQUIRED JSON Response Format:
{{
    "name": "Clear, business-oriented name (max 50 chars)",
    "description": "What this KPI measures and why it matters (max 200 chars)",
    "sql_calculation": "Complete SQL query returning one 'value' column",
    "column_used": "Primary column used in calculation", 
    "calculation_type": "sum|avg|count|percentage|ratio",
    "format_type": "currency|percentage|number|decimal",
    "business_impact": "How this KPI helps make business decisions",
    "expected_range": "What range of values would be typical"
}}

EXAMPLES for {domain}:
{self._get_domain_specific_examples(domain, numeric_columns)}"""

            # Make LLM request with retries - use reasoning model for complex analysis
            response = await self.llm_client._make_llm_request_with_reasoning(
                user_prompt=f"{system_prompt}\n\n{user_prompt}",  # Combine for gpt-4o-mini
                system_prompt=None,  # gpt-4o-mini doesn't use system prompts
                temperature=0.7,  # gpt-4o-mini uses its own temperature
            )

            if not response:
                logger.error(f"No response from LLM for KPI {kpi_index}")
                return None

            # Parse and validate response
            kpi_config = self._parse_and_validate_kpi_response(
                response, kpi_index, domain
            )

            if kpi_config:
                logger.info(
                    f"Successfully generated KPI {kpi_index}: {kpi_config.name}"
                )

            return kpi_config

        except Exception as e:
            logger.error(f"Error generating KPI {kpi_index}: {e}", exc_info=True)
            return None

    def _get_domain_specific_examples(
        self, domain: str, numeric_columns: List[str]
    ) -> str:
        """Get domain-specific examples for better LLM guidance with DuckDB syntax."""
        examples = {
            "ecommerce": [
                '{"name": "Total Revenue", "sql_calculation": "SELECT COALESCE(SUM(total_amount), 0) as value FROM dataset WHERE total_amount > 0", "format_type": "currency"}',
                '{"name": "Average Order Value", "sql_calculation": "SELECT COALESCE(AVG(total_amount), 0) as value FROM dataset WHERE total_amount > 0", "format_type": "currency"}',
                '{"name": "Recent Orders Rate", "sql_calculation": "SELECT (COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL \'7\' DAY THEN 1 END) * 100.0 / COUNT(*)) as value FROM dataset WHERE order_date IS NOT NULL", "format_type": "percentage"}',
            ],
            "finance": [
                '{"name": "Total Assets", "sql_calculation": "SELECT COALESCE(SUM(amount), 0) as value FROM dataset WHERE amount > 0", "format_type": "currency"}',
                '{"name": "Average Transaction", "sql_calculation": "SELECT COALESCE(AVG(amount), 0) as value FROM dataset WHERE amount > 0", "format_type": "currency"}',
                '{"name": "Recent Transactions", "sql_calculation": "SELECT COUNT(*) as value FROM dataset WHERE transaction_date >= CURRENT_DATE - INTERVAL \'30\' DAY", "format_type": "number"}',
            ],
            "saas": [
                '{"name": "Monthly Recurring Revenue", "sql_calculation": "SELECT COALESCE(SUM(revenue), 0) as value FROM dataset WHERE revenue > 0", "format_type": "currency"}',
                '{"name": "Active User Engagement", "sql_calculation": "SELECT COALESCE(AVG(pages_viewed), 0) as value FROM dataset WHERE subscription_status = \'Active\' AND pages_viewed IS NOT NULL", "format_type": "number"}',
                '{"name": "Recent Feature Usage", "sql_calculation": "SELECT COUNT(DISTINCT user_id) as value FROM dataset WHERE activity_date >= CURRENT_DATE - INTERVAL \'7\' DAY", "format_type": "number"}',
            ],
        }

        domain_examples = examples.get(domain, examples["ecommerce"])
        return "\n".join(domain_examples)

    def _extract_json_from_response(self, response: str) -> str:
        """Extract JSON content from LLM response, handling markdown code blocks."""
        response_content = response.strip()

        # Check if response is wrapped in markdown code blocks
        if response_content.startswith("```json") and response_content.endswith("```"):
            # Extract JSON content from between the code blocks
            json_start = response_content.find("```json") + 7  # Length of '```json'
            json_end = response_content.rfind("```")
            response_content = response_content[json_start:json_end].strip()
        elif response_content.startswith("```") and response_content.endswith("```"):
            # Handle generic code blocks
            json_start = response_content.find("```") + 3
            json_end = response_content.rfind("```")
            response_content = response_content[json_start:json_end].strip()

        return response_content

    def _parse_and_validate_kpi_response(
        self, response: str, kpi_index: int, domain: str
    ) -> Optional[KPIConfig]:
        """Parse and validate LLM response for KPI generation."""

        try:
            # Log the raw response
            logger.info(f"LLM Response for KPI {kpi_index}:")
            logger.info(f"Raw response: {response[:500]}...")  # Log first 500 chars

            # Parse JSON
            try:
                response_content = self._extract_json_from_response(response)
                kpi_data = json.loads(response_content)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse LLM response as JSON: {e}")
                logger.error(f"Response content: {response}")
                return None

            # Log parsed data
            logger.info(f"Parsed KPI data: {kpi_data}")

            # Validate required fields
            required_fields = [
                "name",
                "description",
                "sql_calculation",
                "column_used",
                "calculation_type",
                "format_type",
            ]
            missing_fields = [
                field for field in required_fields if field not in kpi_data
            ]

            if missing_fields:
                logger.error(f"LLM response missing required fields: {missing_fields}")
                logger.error(f"Available fields: {list(kpi_data.keys())}")
                return None

            # Validate field values
            if not kpi_data["name"] or len(kpi_data["name"]) > 100:
                logger.error(f"Invalid KPI name: {kpi_data['name']}")
                return None

            if (
                not kpi_data["sql_calculation"]
                or "SELECT" not in kpi_data["sql_calculation"].upper()
            ):
                logger.error(f"Invalid SQL calculation: {kpi_data['sql_calculation']}")
                return None

            if "value" not in kpi_data["sql_calculation"].lower():
                logger.warning(
                    f"SQL query might not return 'value' column: {kpi_data['sql_calculation']}"
                )

            # Create KPI configuration
            kpi_config = KPIConfig(
                id=f"kpi_{kpi_index}",
                name=kpi_data["name"][:50],  # Truncate if too long
                description=kpi_data["description"][:200],  # Truncate if too long
                value_column=kpi_data["column_used"],
                calculation=kpi_data["calculation_type"],
                format_type=kpi_data["format_type"],
                color=self.domain_colors[domain][
                    (kpi_index - 1) % len(self.domain_colors[domain])
                ],
                importance="high" if kpi_index <= 2 else "medium",
                explanation=kpi_data.get("business_impact", "")[
                    :300
                ],  # Truncate if too long
                sql_query=kpi_data["sql_calculation"],
            )

            # Final validation
            if not self._validate_kpi_config(kpi_config):
                logger.error("KPI configuration failed final validation")
                return None

            logger.info(f"Successfully created and validated KPI: {kpi_config.name}")
            return kpi_config

        except Exception as e:
            logger.error(f"Error parsing/validating KPI response: {e}", exc_info=True)
            return None

    def _validate_kpi_config(self, kpi_config: KPIConfig) -> bool:
        """Validate KPI configuration for completeness and safety."""
        try:
            # Check required fields
            if not kpi_config.name or not kpi_config.sql_query:
                return False

            # Check SQL safety (basic validation)
            sql_upper = kpi_config.sql_query.upper()
            dangerous_keywords = [
                "DROP",
                "DELETE",
                "UPDATE",
                "INSERT",
                "CREATE",
                "ALTER",
            ]
            if any(keyword in sql_upper for keyword in dangerous_keywords):
                logger.error(
                    f"Dangerous SQL keywords detected in KPI query: {kpi_config.sql_query}"
                )
                return False

            # Check format type
            valid_formats = ["currency", "percentage", "number", "decimal"]
            if kpi_config.format_type not in valid_formats:
                logger.warning(
                    f"Unknown format type: {kpi_config.format_type}, defaulting to 'number'"
                )
                kpi_config.format_type = "number"

            return True

        except Exception as e:
            logger.error(f"Error validating KPI config: {e}")
            return False

    async def _generate_charts(
        self,
        domain: str,
        profile_summary: Dict[str, Any],
        kpis: List[KPIConfig],
        profile: DataProfile,
    ) -> List[ChartConfig]:
        """Generate chart configurations using enhanced LLM calls with data context."""
        try:
            logger.info(
                f"Generating charts for {domain} domain with enhanced data context"
            )

            # Get sample data for context
            sample_data = self._get_sample_data_for_llm(profile_summary)

            # Generate each chart individually with proper context
            charts: List[ChartConfig] = []
            target_chart_count = 4  # Generate up to 4 charts

            for i in range(target_chart_count):
                try:
                    chart_config = await self._generate_single_chart(
                        domain=domain,
                        profile_summary=profile_summary,
                        sample_data=sample_data,
                        existing_kpis=[kpi.name for kpi in kpis],
                        existing_charts=[chart.title for chart in charts],
                        chart_index=i + 1,
                    )
                    if chart_config:
                        charts.append(chart_config)
                        logger.info(f"Generated Chart {i+1}: {chart_config.title}")
                except Exception as e:
                    logger.error(f"Failed to generate Chart {i+1}: {e}")
                    continue

            # If we don't have enough charts, add fallback ones
            if len(charts) < 2:
                logger.warning("Not enough charts generated, adding fallback charts")
                fallback_charts = self._generate_fallback_charts(profile)
                charts.extend(fallback_charts[: 4 - len(charts)])

            return charts[:6]  # Return max 6 charts

        except Exception as e:
            logger.error(
                f"Enhanced chart generation failed: {e}. Using fallback charts."
            )
            return self._generate_fallback_charts(profile)

    async def _generate_single_chart(
        self,
        domain: str,
        profile_summary: Dict[str, Any],
        sample_data: str,
        existing_kpis: List[str],
        existing_charts: List[str],
        chart_index: int,
    ) -> Optional[ChartConfig]:
        """Generate a single chart with data feasibility validation."""

        try:
            # Validate inputs and data availability
            if not domain or not profile_summary:
                logger.error("Invalid inputs for chart generation")
                return None

            # Get column information for validation
            numeric_columns = profile_summary.get("numeric_columns", [])
            categorical_columns = profile_summary.get("categorical_columns", [])
            datetime_columns = profile_summary.get("datetime_columns", [])
            total_rows = profile_summary.get("total_rows", 0)

            # Check if we have enough data for meaningful charts
            if total_rows < 5:
                logger.warning(
                    f"Insufficient data rows ({total_rows}) for chart generation"
                )
                return None

            # Determine chart feasibility based on available columns
            chart_options = self._get_feasible_chart_options(
                numeric_columns, categorical_columns, datetime_columns, existing_charts
            )

            if not chart_options:
                logger.warning("No feasible chart options available")
                return None

            system_prompt = f"""You are a data visualization expert for {domain} analytics.
Your task is to design ONE chart that WILL DEFINITELY WORK and provide actionable business insights.

CRITICAL SUCCESS REQUIREMENTS:
1. Charts MUST use available data columns - no made-up column names
2. SQL queries MUST return data that can be visualized
3. Choose the SIMPLEST chart type that conveys the message
4. Focus on WORKING charts over complex visualizations

CHART TYPE SELECTION (choose the most reliable):
- PIE: Best for categorical breakdowns - needs categorical column and COUNT/SUM
- BAR: Best for comparing categories - needs categorical column and aggregation
- LINE: For trends over time - needs datetime or sequential numeric data
- SCATTER: Only if you have 2+ numeric columns for correlation
- AREA: Similar to line but for cumulative data

IMPORTANT: Use DuckDB SQL syntax (NOT MySQL/PostgreSQL):
- Date functions: CURRENT_DATE (not CURDATE())
- Date arithmetic: CURRENT_DATE - INTERVAL '30' DAY (not DATE_SUB)
- Date casting: CAST(column AS DATE) (not DATE(column))
- Keep queries SIMPLE and RELIABLE

SQL RELIABILITY RULES:
- Use COUNT(*) for simple counting (most reliable)
- Use SUM() only on confirmed numeric columns
- Always include WHERE clauses to filter NULL values
- Keep GROUP BY simple with single columns
- LIMIT results to prevent overcrowding (LIMIT 10 for categories)
- Test that column names exist in the actual data

BUSINESS VALUE FOCUS:
- Each chart must answer a specific business question
- Provide insights that drive decision-making
- Choose metrics that matter for {domain} domain"""

            user_prompt = f"""Dataset Overview:
- Domain: {domain}
- Total Rows: {total_rows:,}
- Available Chart Options: {', '.join(chart_options)}

Column Analysis:
{sample_data}

Numeric Columns: {', '.join(numeric_columns[:5])} ({len(numeric_columns)} total)
Categorical Columns: {', '.join(categorical_columns[:5])} ({len(categorical_columns)} total)
DateTime Columns: {', '.join(datetime_columns[:3])} ({len(datetime_columns)} total)

Existing KPIs: {', '.join(existing_kpis) if existing_kpis else 'None'}
Existing Charts: {', '.join(existing_charts) if existing_charts else 'None'}

Create a SIMPLE chart that will definitely work. Focus on:
1. Using basic COUNT(*) queries when possible (most reliable)
2. Simple categorical breakdowns (like "status distribution", "category breakdown")
3. Avoid complex date filtering or advanced aggregations
4. Keep SQL queries simple and safe

REQUIRED JSON Response (no extra text):
{{
    "name": "Simple descriptive chart title",
    "description": "What this chart shows",
    "chart_type": "pie|bar|line",
    "x_axis": "actual_column_name_from_data",
    "y_axis": "actual_column_name_or_null_for_pie",
    "sql_query": "SELECT column, COUNT(*) as count FROM dataset WHERE column IS NOT NULL GROUP BY column ORDER BY count DESC LIMIT 10",
    "business_value": "Simple business insight this provides"
}}

Examples of SIMPLE, WORKING queries:
- Pie chart: "SELECT status, COUNT(*) as count FROM dataset WHERE status IS NOT NULL GROUP BY status LIMIT 8"
- Bar chart: "SELECT category, COUNT(*) as count FROM dataset WHERE category IS NOT NULL GROUP BY category ORDER BY count DESC LIMIT 10"
- Line chart: "SELECT date_column, COUNT(*) as count FROM dataset WHERE date_column IS NOT NULL GROUP BY date_column ORDER BY date_column"

Choose the simplest chart type that will work with your data."""

            # Make LLM request with validation - use reasoning model for complex chart analysis
            response = await self.llm_client._make_llm_request_with_reasoning(
                user_prompt=f"{system_prompt}\n\n{user_prompt}",  # Combine for gpt-4o-mini
                system_prompt=None,  # gpt-4o-mini doesn't use system prompts
                temperature=0.7,  # gpt-4o-mini uses its own temperature
            )

            if not response:
                logger.error(f"No response from LLM for chart {chart_index}")
                return None

            # Parse and validate chart response
            chart_config = self._parse_and_validate_chart_response(
                response, chart_index, domain, chart_options, profile_summary
            )

            if chart_config:
                logger.info(
                    f"Successfully generated chart {chart_index}: {chart_config.title}"
                )

            return chart_config

        except Exception as e:
            logger.error(f"Error generating chart {chart_index}: {e}", exc_info=True)
            return None

    def _get_feasible_chart_options(
        self,
        numeric_columns: List[str],
        categorical_columns: List[str],
        datetime_columns: List[str],
        existing_charts: List[str],
    ) -> List[str]:
        """Determine which chart types are feasible with available data, optimized for success."""

        feasible_options = []

        # Always include bar charts if we have any categorical data
        if categorical_columns:
            feasible_options.append("bar")

        # Always include pie charts if we have categorical data (most flexible)
        if categorical_columns:
            feasible_options.append("pie")

        # Include line charts if we have datetime OR numeric data (more flexible)
        if datetime_columns or numeric_columns:
            feasible_options.append("line")

        # Include scatter plots only if we have multiple numeric columns
        if len(numeric_columns) >= 2:
            feasible_options.append("scatter")

        # Area charts similar to line charts
        if datetime_columns or numeric_columns:
            feasible_options.append("area")

        # If no specific options, default to most flexible ones
        if not feasible_options:
            feasible_options = ["bar", "pie"]  # Most forgiving chart types

        logger.info(f"Feasible chart options: {feasible_options}")
        return feasible_options

    def _get_chart_examples_for_domain(self, domain: str) -> str:
        """Get domain-specific chart examples with DuckDB syntax - focused on reliability."""

        examples = {
            "ecommerce": [
                '{"name": "Product Category Distribution", "chart_type": "pie", "sql_query": "SELECT product_category, COUNT(*) as count FROM dataset WHERE product_category IS NOT NULL GROUP BY product_category ORDER BY count DESC LIMIT 8"}',
                '{"name": "Order Status Breakdown", "chart_type": "bar", "sql_query": "SELECT order_status, COUNT(*) as orders FROM dataset WHERE order_status IS NOT NULL GROUP BY order_status ORDER BY orders DESC LIMIT 10"}',
                '{"name": "Daily Order Count", "chart_type": "line", "sql_query": "SELECT CAST(order_date AS DATE) as date, COUNT(*) as daily_orders FROM dataset WHERE order_date IS NOT NULL GROUP BY CAST(order_date AS DATE) ORDER BY date LIMIT 30"}',
            ],
            "finance": [
                '{"name": "Transaction Type Distribution", "chart_type": "pie", "sql_query": "SELECT transaction_type, COUNT(*) as count FROM dataset WHERE transaction_type IS NOT NULL GROUP BY transaction_type ORDER BY count DESC LIMIT 8"}',
                '{"name": "Account Type Breakdown", "chart_type": "bar", "sql_query": "SELECT account_type, COUNT(*) as accounts FROM dataset WHERE account_type IS NOT NULL GROUP BY account_type ORDER BY accounts DESC LIMIT 10"}',
                '{"name": "Category Distribution", "chart_type": "pie", "sql_query": "SELECT category, COUNT(*) as count FROM dataset WHERE category IS NOT NULL GROUP BY category ORDER BY count DESC LIMIT 8"}',
            ],
            "saas": [
                '{"name": "Plan Type Distribution", "chart_type": "pie", "sql_query": "SELECT plan_type, COUNT(*) as users FROM dataset WHERE plan_type IS NOT NULL GROUP BY plan_type ORDER BY users DESC LIMIT 8"}',
                '{"name": "Feature Usage", "chart_type": "bar", "sql_query": "SELECT feature_used, COUNT(*) as usage_count FROM dataset WHERE feature_used IS NOT NULL GROUP BY feature_used ORDER BY usage_count DESC LIMIT 10"}',
                '{"name": "User Status Distribution", "chart_type": "pie", "sql_query": "SELECT subscription_status, COUNT(*) as count FROM dataset WHERE subscription_status IS NOT NULL GROUP BY subscription_status ORDER BY count DESC"}',
            ],
        }

        domain_examples = examples.get(domain, examples["ecommerce"])
        return "\n".join(domain_examples)

    def _parse_and_validate_chart_response(
        self,
        response: str,
        chart_index: int,
        domain: str,
        feasible_options: List[str],
        profile_summary: Dict[str, Any],
    ) -> Optional[ChartConfig]:
        """Parse and validate LLM response for chart generation."""

        try:
            # Log the raw response
            logger.info(f"LLM Response for Chart {chart_index}:")
            logger.info(f"Raw response: {response[:500]}...")

            # Parse JSON
            try:
                response_content = self._extract_json_from_response(response)
                chart_data = json.loads(response_content)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse chart LLM response as JSON: {e}")
                logger.error(f"Response content: {response}")
                return None

            # Log parsed data
            logger.info(f"Parsed chart data: {chart_data}")

            # Validate required fields
            required_fields = ["name", "description", "chart_type", "sql_query"]
            missing_fields = [
                field for field in required_fields if field not in chart_data
            ]

            if missing_fields:
                logger.error(
                    f"Chart LLM response missing required fields: {missing_fields}"
                )
                return None

            # Validate chart type feasibility
            chart_type = chart_data.get("chart_type", "").lower()
            if chart_type not in feasible_options:
                logger.error(
                    f"Chart type '{chart_type}' not feasible. Options: {feasible_options}"
                )
                return None

            # Validate SQL query
            sql_query = chart_data.get("sql_query", "")
            if not self._validate_chart_sql(sql_query, chart_type, profile_summary):
                logger.error(f"Invalid SQL query for chart: {sql_query}")
                return None

            # Create chart configuration
            chart_config = ChartConfig(
                id=f"chart_{chart_index}",
                type=ChartType(chart_type),
                title=chart_data["name"][:60],  # Truncate if too long
                description=chart_data["description"][:200],
                x_axis=chart_data.get("x_axis", ""),
                y_axis=chart_data.get("y_axis", ""),
                explanation=chart_data.get("business_value", "")[:300],
                sql_query=sql_query,
            )

            # Final validation
            if not self._validate_chart_config(chart_config, profile_summary):
                logger.error("Chart configuration failed final validation")
                return None

            logger.info(
                f"Successfully created and validated chart: {chart_config.title}"
            )
            return chart_config

        except Exception as e:
            logger.error(f"Error parsing/validating chart response: {e}", exc_info=True)
            return None

    def _validate_chart_sql(
        self, sql_query: str, chart_type: str, profile_summary: Dict[str, Any]
    ) -> bool:
        """Validate SQL query is appropriate for chart type and data."""

        try:
            if not sql_query or "SELECT" not in sql_query.upper():
                return False

            sql_upper = sql_query.upper()
            all_columns = [
                col.get("name", "") for col in profile_summary.get("columns", [])
            ]

            # Basic safety checks
            dangerous_keywords = [
                "DROP",
                "DELETE",
                "UPDATE",
                "INSERT",
                "CREATE",
                "ALTER",
            ]
            if any(keyword in sql_upper for keyword in dangerous_keywords):
                return False

            # Chart-specific validations
            if chart_type == "pie":
                # Pie charts should have GROUP BY and reasonable limits
                if "GROUP BY" not in sql_upper:
                    logger.warning("Pie chart SQL should have GROUP BY")
                    return False

            elif chart_type in ["line", "area"]:
                # Time series charts should order by date/time
                if "ORDER BY" not in sql_upper:
                    logger.warning("Time series charts should have ORDER BY")
                    return False

            elif chart_type == "bar":
                # Bar charts should have GROUP BY for categories
                if "GROUP BY" not in sql_upper:
                    logger.warning("Bar chart SQL should have GROUP BY")
                    return False

            # Check for NULL handling
            if "IS NOT NULL" not in sql_upper and "COALESCE" not in sql_upper:
                logger.warning("SQL query should handle NULL values")

            return True

        except Exception as e:
            logger.error(f"Error validating chart SQL: {e}")
            return False

    def _validate_chart_config(
        self, chart_config: ChartConfig, profile_summary: Dict[str, Any]
    ) -> bool:
        """Final validation of chart configuration."""

        try:
            # Check required fields
            if not chart_config.title or not chart_config.sql_query:
                return False

            # Validate chart type
            valid_chart_types = ["bar", "line", "pie", "scatter", "area"]
            if chart_config.type.value not in valid_chart_types:
                logger.error(f"Invalid chart type: {chart_config.type.value}")
                return False

            # Check if referenced columns exist
            all_columns = [
                col.get("name", "").lower()
                for col in profile_summary.get("columns", [])
            ]

            if chart_config.x_axis and chart_config.x_axis.lower() not in all_columns:
                logger.warning(
                    f"X-axis column '{chart_config.x_axis}' not found in dataset"
                )
                # Don't fail for this, LLM might use aggregated columns

            if chart_config.y_axis and chart_config.y_axis.lower() not in all_columns:
                logger.warning(
                    f"Y-axis column '{chart_config.y_axis}' not found in dataset"
                )
                # Don't fail for this, LLM might use aggregated columns

            return True

        except Exception as e:
            logger.error(f"Error validating chart config: {e}")
            return False

    async def _generate_filters(self, profile: DataProfile) -> List[FilterConfig]:
        """Generate intelligent filter configurations using GPT analysis."""
        try:
            logger.info("Generating intelligent filters using GPT")

            # Prepare data for GPT analysis
            filter_context = self._prepare_filter_context(profile)

            # Generate filters using GPT
            gpt_filters = await self._generate_gpt_filters(filter_context, profile)

            # Combine with basic filters as fallback
            basic_filters = self._generate_basic_filters(profile)

            # Merge and deduplicate
            all_filters = gpt_filters + basic_filters
            unique_filters = self._deduplicate_filters(all_filters)

            logger.info(f"Generated {len(unique_filters)} intelligent filters")
            return unique_filters[:5]  # Limit to 5 filters

        except Exception as e:
            logger.error(f"GPT filter generation failed: {e}. Using basic filters.")
            return self._generate_basic_filters(profile)

    def _prepare_filter_context(self, profile: DataProfile) -> str:
        """Prepare context for GPT filter generation."""
        context_parts = []

        # Add categorical columns with sample values
        if profile.categorical_columns:
            context_parts.append("CATEGORICAL COLUMNS:")
            for col_name in profile.categorical_columns[:5]:
                col_profile = next(
                    (col for col in profile.columns if col.name == col_name), None
                )
                if col_profile and col_profile.top_values:
                    top_vals = [v["value"] for v in col_profile.top_values[:3]]
                    context_parts.append(
                        f"- {col_name}: {', '.join(top_vals)} (and {col_profile.unique_count} total values)"
                    )

        # Add datetime columns
        if profile.datetime_columns:
            context_parts.append("\nDATETIME COLUMNS:")
            for col_name in profile.datetime_columns[:3]:
                col_profile = next(
                    (col for col in profile.columns if col.name == col_name), None
                )
                if col_profile:
                    context_parts.append(
                        f"- {col_name}: {col_profile.min_value} to {col_profile.max_value}"
                    )

        # Add numeric columns
        if profile.numeric_columns:
            context_parts.append("\nNUMERIC COLUMNS:")
            for col_name in profile.numeric_columns[:3]:
                col_profile = next(
                    (col for col in profile.columns if col.name == col_name), None
                )
                if col_profile:
                    context_parts.append(
                        f"- {col_name}: {col_profile.min_value} to {col_profile.max_value}"
                    )

        return "\n".join(context_parts)

    async def _generate_gpt_filters(
        self, context: str, profile: DataProfile
    ) -> List[FilterConfig]:
        """Use GPT to generate intelligent filters."""
        try:
            system_prompt = """You are a business intelligence expert who creates useful data filters.
Your task is to generate practical filters that business users would actually want to use.

FILTER TYPES AVAILABLE:
1. "categorical" - For selecting specific values (like status, category, type)
2. "date_range" - For filtering by date ranges (like last 30 days, this quarter)
3. "numeric_range" - For filtering by numeric ranges (like amount ranges, counts)
4. "multi_select" - For selecting multiple values from a list

REQUIREMENTS:
- Generate 2-4 filters maximum
- Focus on business-relevant filters that provide actionable insights
- Choose columns that users would commonly want to filter by
- Use clear, business-friendly filter names

STRICT JSON OUTPUT (no extra text):
[
    {
        "name": "Business-friendly filter name",
        "column": "actual_column_name",
        "type": "categorical|date_range|numeric_range|multi_select",
        "description": "What this filter does"
    }
]"""

            user_prompt = f"""Dataset Information:
- Total Rows: {profile.total_rows:,}
- Domain Context: Business data analysis

Available Columns for Filtering:
{context}

Generate useful business filters that would help users analyze this data effectively.
Focus on the most commonly filtered columns in business analysis."""

            response = await self.llm_client._make_llm_request_with_reasoning(
                user_prompt=f"{system_prompt}\n\n{user_prompt}",
                system_prompt=None,
                temperature=0.7,
                use_reasoning=True,
            )

            if not response:
                logger.error("No response from GPT for filter generation")
                return []

            # Parse GPT response
            try:
                response_content = self._extract_json_from_response(response)
                filter_configs = json.loads(response_content)

                if not isinstance(filter_configs, list):
                    logger.error("GPT response is not a list")
                    return []

                gpt_filters = []
                for i, filter_data in enumerate(filter_configs):
                    try:
                        filter_config = self._create_filter_from_gpt_data(
                            filter_data, profile, i
                        )
                        if filter_config:
                            gpt_filters.append(filter_config)
                    except Exception as e:
                        logger.error(f"Error creating filter {i}: {e}")
                        continue

                logger.info(f"Successfully created {len(gpt_filters)} GPT filters")
                return gpt_filters

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse GPT filter response: {e}")
                return []

        except Exception as e:
            logger.error(f"Error generating GPT filters: {e}")
            return []

    def _create_filter_from_gpt_data(
        self, filter_data: dict, profile: DataProfile, index: int
    ) -> Optional[FilterConfig]:
        """Create a FilterConfig from GPT-generated data."""
        try:
            # Validate required fields
            if not all(key in filter_data for key in ["name", "column", "type"]):
                logger.error(f"Missing required fields in filter data: {filter_data}")
                return None

            column_name = filter_data["column"]
            filter_type = filter_data["type"]

            # Validate column exists
            if not any(col.name == column_name for col in profile.columns):
                logger.error(f"Column '{column_name}' not found in profile")
                return None

            # Get column profile for additional data
            col_profile = next(
                (col for col in profile.columns if col.name == column_name), None
            )

            # Create appropriate filter type
            filter_type_mapping = {
                "categorical": FilterType.CATEGORICAL,
                "date_range": FilterType.DATE_RANGE,
                "numeric_range": FilterType.NUMERIC_RANGE,
                "multi_select": FilterType.MULTI_SELECT,
            }

            if filter_type not in filter_type_mapping:
                logger.error(f"Invalid filter type: {filter_type}")
                return None

            # Prepare filter options
            options = []
            default_value = None

            if filter_type in ["categorical", "multi_select"] and col_profile:
                options = [v["value"] for v in col_profile.top_values[:10]]
            elif filter_type == "numeric_range" and col_profile:
                default_value = {
                    "min": col_profile.min_value,
                    "max": col_profile.max_value,
                }

            return FilterConfig(
                id=f"gpt_filter_{index}",
                name=filter_data["name"],
                column=column_name,
                type=filter_type_mapping[filter_type],
                default_value=default_value,
                options=options,
                is_global=True,
            )

        except Exception as e:
            logger.error(f"Error creating filter config: {e}")
            return None

    def _generate_basic_filters(self, profile: DataProfile) -> List[FilterConfig]:
        """Generate basic fallback filters."""
        filters = []

        # Add date filter if datetime columns exist
        if profile.datetime_columns:
            date_col = profile.datetime_columns[0]
            filters.append(
                FilterConfig(
                    id="date_filter",
                    name="Date Range",
                    column=date_col,
                    type=FilterType.DATE_RANGE,
                    is_global=True,
                )
            )

        # Add categorical filters for low-cardinality columns
        for col_name in profile.categorical_columns[:2]:
            col_profile = next(
                (col for col in profile.columns if col.name == col_name), None
            )
            if col_profile and col_profile.unique_count <= 15:  # Reasonable for filters
                filters.append(
                    FilterConfig(
                        id=f"filter_{col_name}",
                        name=col_profile.original_name.replace("_", " ").title(),
                        column=col_name,
                        type=FilterType.MULTI_SELECT,
                        options=(
                            [v["value"] for v in col_profile.top_values]
                            if col_profile.top_values
                            else []
                        ),
                        is_global=True,
                    )
                )

        return filters

    def _deduplicate_filters(self, filters: List[FilterConfig]) -> List[FilterConfig]:
        """Remove duplicate filters based on column name."""
        seen_columns = set()
        unique_filters = []

        for filter_config in filters:
            if filter_config.column not in seen_columns:
                seen_columns.add(filter_config.column)
                unique_filters.append(filter_config)

        return unique_filters

    async def _generate_layout(
        self,
        kpis: List[KPIConfig],
        charts: List[ChartConfig],
        filters: List[FilterConfig],
    ) -> LayoutConfig:
        """Generate layout configuration."""
        return LayoutConfig(
            kpi_section={
                "position": "top",
                "height": "auto",
                "columns": min(len(kpis), 4),
            },
            chart_section={"position": "middle", "grid_type": "responsive", "gap": 16},
            filter_section={"position": "sidebar", "width": 250, "collapsible": True},
        )

    def _generate_title_description(
        self, domain_info: DomainClassification, profile: DataProfile
    ) -> tuple[str, str]:
        """Generate dashboard title and description."""
        domain_titles = {
            "ecommerce": "E-commerce Analytics Dashboard",
            "finance": "Financial Analytics Dashboard",
            "manufacturing": "Manufacturing Operations Dashboard",
            "saas": "SaaS Metrics Dashboard",
            "generic": "Data Analytics Dashboard",
        }

        title = domain_titles.get(domain_info.domain, "Analytics Dashboard")

        description = (
            f"Auto-generated dashboard for {domain_info.domain} data analysis. "
            f"Analyzing {profile.total_rows:,} rows across {profile.total_columns} columns."
        )

        return title, description

    def _find_best_column_match(
        self, suggested_columns: List[str], profile: DataProfile
    ) -> str:
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
                if (
                    suggested.lower() in available.lower()
                    or available.lower() in suggested.lower()
                ):
                    return available

        # Return first numeric column as fallback
        if profile.numeric_columns:
            return profile.numeric_columns[0]

        return ""

    def _validate_calculation_for_column(
        self, column_name: str, calculation: str, profile: DataProfile
    ) -> str:
        """Validate that the calculation is appropriate for the column type."""
        if not column_name or column_name == "*":
            # COUNT(*) is always valid
            return "count"

        # Find the column profile
        column_profile = next(
            (col for col in profile.columns if col.name == column_name), None
        )
        if not column_profile:
            return "count"

        # Check if calculation is appropriate for column type
        numeric_calculations = {"sum", "avg", "average", "mean", "min", "max"}

        if calculation.lower() in numeric_calculations:
            # Only allow numeric calculations on numeric columns
            if column_profile.data_type.value == "numeric":
                return calculation.lower()
            else:
                # For non-numeric columns, default to count
                return "count"

        # COUNT is valid for all column types
        if calculation.lower() in {"count", "cnt"}:
            return "count"

        # Default to count for unknown calculations
        return "count"

    def _infer_format_type(self, kpi_name: str, domain: str) -> str:
        """Infer the format type for a KPI based on its name and domain."""
        name_lower = kpi_name.lower()

        if any(
            word in name_lower
            for word in ["revenue", "sales", "cost", "price", "amount", "$"]
        ):
            return "currency"
        elif any(word in name_lower for word in ["rate", "percentage", "%", "ratio"]):
            return "percentage"
        elif any(
            word in name_lower for word in ["count", "number", "quantity", "total"]
        ):
            return "number"
        else:
            return "number"

    def _generate_fallback_kpis(
        self, profile: DataProfile, domain: str
    ) -> List[KPIConfig]:
        """Generate fallback KPIs when LLM fails."""
        kpis = []

        # Total count KPI
        kpis.append(
            KPIConfig(
                id="kpi_count",
                name="Total Records",
                description="Total number of records in the dataset",
                value_column="*",
                calculation="count",
                format_type="number",
                color=self.domain_colors[domain][0],
                explanation="Basic count of all records in the dataset",
            )
        )

        # Numeric column KPI if available
        if profile.numeric_columns:
            col_name = profile.numeric_columns[0]
            # Use validation to ensure proper calculation
            calculation = self._validate_calculation_for_column(
                col_name, "sum", profile
            )
            kpis.append(
                KPIConfig(
                    id="kpi_numeric",
                    name=(
                        f"Total {col_name}"
                        if calculation == "sum"
                        else f"{calculation.title()} {col_name}"
                    ),
                    description=f"{calculation.title()} of {col_name}",
                    value_column=col_name,
                    calculation=calculation,
                    format_type="number",
                    color=self.domain_colors[domain][1],
                    explanation=f"{calculation.title()} of all values in {col_name}",
                )
            )

        return kpis

    def _generate_fallback_charts(self, profile: DataProfile) -> List[ChartConfig]:
        """Generate fallback charts when LLM fails."""
        charts = []

        # Bar chart for categorical data
        if profile.categorical_columns and profile.numeric_columns:
            # Validate aggregation for the y-axis column
            y_column = profile.numeric_columns[0]
            validated_aggregation = self._validate_calculation_for_column(
                y_column, "sum", profile
            )

            charts.append(
                ChartConfig(
                    id="chart_bar",
                    type=ChartType.BAR,
                    title=f"{profile.categorical_columns[0]} Distribution",
                    description=f"Distribution of {profile.categorical_columns[0]}",
                    x_axis=profile.categorical_columns[0],
                    y_axis=y_column,
                    aggregation=validated_aggregation,
                    width=6,
                    height=4,
                )
            )

        # Time series chart if datetime available
        if profile.datetime_columns and profile.numeric_columns:
            # Validate aggregation for the y-axis column
            y_column = profile.numeric_columns[0]
            validated_aggregation = self._validate_calculation_for_column(
                y_column, "sum", profile
            )

            charts.append(
                ChartConfig(
                    id="chart_timeseries",
                    type=ChartType.LINE,
                    title="Time Series Analysis",
                    description="Trend over time",
                    x_axis=profile.datetime_columns[0],
                    y_axis=y_column,
                    aggregation=validated_aggregation,
                    width=6,
                    height=4,
                )
            )

        return charts
