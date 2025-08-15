"""LLM client for OpenAI GPT-4 integration."""

import json
import openai
from typing import Dict, List, Any, Optional, TypeVar, Type
import logging
import asyncio
from datetime import datetime
from pydantic import BaseModel

from backend.config import get_settings
from backend.utils.exceptions import LLMException

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)
settings = get_settings()


class LLMClient:
    """Client for interacting with OpenAI GPT models."""

    def __init__(self):
        self.client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model
        self.reasoning_model = "gpt-4.1-mini"  # For complex reasoning tasks
        self.max_retries = 3
        self.retry_delay = 1.0

    async def classify_domain(self, prompt: str) -> Dict[str, Any]:
        """
        Classify the business domain using LLM.

        Args:
            prompt: Formatted prompt for domain classification

        Returns:
            Classification result dictionary
        """
        system_prompt = """You are a data domain expert specializing in business data analysis. 
Your task is to analyze dataset structures and classify them into appropriate business domains.

Focus on:
1. Column names and their business meaning
2. Data types and their typical usage patterns  
3. Relationships between different data fields
4. Common business processes reflected in the data

Always respond with valid JSON in the exact format requested."""

        try:
            response = await self._make_llm_request(
                system_prompt=system_prompt, user_prompt=prompt, temperature=0.3
            )

            # Parse JSON response
            result = json.loads(response)

            # Validate required fields
            required_fields = ["domain", "confidence", "reasoning"]
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")

            return result

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {e}")
            raise LLMException("LLM returned invalid JSON response")
        except Exception as e:
            logger.error(f"Error in natural language parsing: {e}")
            raise LLMException(f"Failed to parse natural language query: {str(e)}")

    async def parse_natural_language_query_enhanced(
        self, 
        query: str, 
        available_columns: List[str], 
        domain: str,
        profile_summary: Dict[str, Any],
        sample_data_context: str
    ) -> Dict[str, Any]:
        """
        Enhanced natural language query parsing with same context as dashboard generation.
        
        Args:
            query: Natural language query from user
            available_columns: List of available column names
            domain: Business domain context
            profile_summary: Rich data profile summary
            sample_data_context: Sample data for LLM context
            
        Returns:
            Parsed query with execution plan
        """
        system_prompt = f"""You are a data visualization expert specializing in {domain} dashboards.
Your task is to interpret natural language queries and convert them into specific, EXECUTABLE chart configurations.

⚠️ CRITICAL RULE: You can ONLY use these exact column names (copy exactly as written):
{chr(10).join([f"  - {col}" for col in available_columns])}

DO NOT use any other column names! If you use a column name not in the list above, the chart will fail.

DOMAIN CONTEXT: {domain}
TOTAL ROWS: {profile_summary.get('total_rows', 0):,}
TOTAL COLUMNS: {profile_summary.get('total_columns', 0)}

COLUMN TYPES AVAILABLE:
- Numeric Columns: {', '.join(profile_summary.get('numeric_columns', []))}
- Categorical Columns: {', '.join(profile_summary.get('categorical_columns', []))}
- DateTime Columns: {', '.join(profile_summary.get('datetime_columns', []))}

SAMPLE DATA CONTEXT:
{sample_data_context}

STRICT COLUMN MAPPING RULES:
- For "revenue"/"sales"/"amount" queries → Use: {[col for col in available_columns if any(word in col.lower() for word in ['amount', 'price', 'revenue', 'total'])]}
- For "time"/"date" queries → Use: {[col for col in available_columns if 'date' in col.lower() or 'time' in col.lower()]}
- For "category"/"type" queries → Use: {[col for col in available_columns if 'category' in col.lower() or 'type' in col.lower()]}
- For "method"/"channel" queries → Use: {[col for col in available_columns if 'method' in col.lower() or 'channel' in col.lower()]}

⚠️ VALIDATION REQUIREMENT: Before responding, double-check that x_axis, y_axis, and color_by values are EXACTLY from this list:
{available_columns}"""

        user_prompt = f"""
USER QUERY: "{query}"

ANALYSIS INSTRUCTIONS:
1. Understand the user's intent and desired visualization
2. Map user terms to ACTUAL column names from the available list
3. Choose appropriate chart type based on data types and relationships
4. Design a chart that will definitely work with the available data
5. For large datasets ({profile_summary.get('total_rows', 0):,} rows), consider aggregation

RESPOND WITH VALID JSON:
{{
    "intent": "visualization|analysis|filter|summary",
    "chart_type": "line|bar|pie|scatter|histogram|heatmap|table",
    "chart_config": {{
        "title": "Clear, descriptive chart title",
        "x_axis": "exact_column_name_from_available_list_or_null",
        "y_axis": "exact_column_name_from_available_list_or_null", 
        "color_by": "exact_column_name_from_available_list_or_null",
        "aggregation": "sum|avg|count|max|min|none",
        "filters": {{}}
    }},
    "execution_steps": [
        "Step 1: Load the {domain} dataset with columns: [list key columns]",
        "Step 2: Apply aggregation/filtering as needed",
        "Step 3: Create {{chart_type}} visualization"
    ],
    "column_mapping": {{
        "user_mentioned": "what user said",
        "mapped_to": "actual_column_name_used",
        "reason": "why this mapping was chosen"
    }},
    "confidence": 0.0-1.0,
    "reasoning": "Why this chart type and configuration will provide valuable insights",
    "data_feasibility": {{
        "estimated_result_rows": "number",
        "aggregation_needed": true/false,
        "chart_complexity": "simple|moderate|complex"
    }}
}}

VALIDATION: Ensure ALL column names in chart_config exist in: {available_columns}"""

        try:
            response = await self.client.chat.completions.create(
                model=self.reasoning_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=2000
            )

            result = json.loads(response.choices[0].message.content)
            
            # STRICT VALIDATION: Reject any non-existent columns
            chart_config = result.get("chart_config", {})
            referenced_columns = [
                chart_config.get("x_axis"),
                chart_config.get("y_axis"), 
                chart_config.get("color_by")
            ]
            
            # Check for any invalid columns and auto-correct them
            corrections_made = False
            for col in referenced_columns:
                if col and col not in available_columns:
                    logger.warning(f"LLM referenced non-existent column: {col}")
                    corrections_made = True
                    # Try to find closest match
                    closest_match = self._find_closest_column_match(col, available_columns)
                    if closest_match:
                        logger.info(f"Auto-correcting {col} to {closest_match}")
                        if chart_config.get("x_axis") == col:
                            chart_config["x_axis"] = closest_match
                        if chart_config.get("y_axis") == col:
                            chart_config["y_axis"] = closest_match
                        if chart_config.get("color_by") == col:
                            chart_config["color_by"] = closest_match
                    else:
                        # Remove invalid column references
                        logger.warning(f"No close match found for {col}, removing reference")
                        if chart_config.get("x_axis") == col:
                            chart_config["x_axis"] = None
                        if chart_config.get("y_axis") == col:
                            chart_config["y_axis"] = None
                        if chart_config.get("color_by") == col:
                            chart_config["color_by"] = None
            
            # Update reasoning if corrections were made
            if corrections_made:
                original_reasoning = result.get("reasoning", "")
                result["reasoning"] = f"CORRECTED: {original_reasoning} [Auto-corrected invalid column references to match available data]"
                result["confidence"] = max(0.6, result.get("confidence", 0.8) - 0.2)  # Reduce confidence for corrected queries

            return result

        except Exception as e:
            logger.error(f"Error in enhanced natural language parsing: {e}")
            raise LLMException(f"Failed to parse enhanced query: {str(e)}")

    def _find_closest_column_match(self, target_col: str, available_columns: List[str]) -> Optional[str]:
        """Find the closest matching column name using smart matching logic."""
        target_lower = target_col.lower()
        
        # Exact match first
        for col in available_columns:
            if target_lower == col.lower():
                return col
        
        # Direct substring matches
        for col in available_columns:
            if target_lower in col.lower() or col.lower() in target_lower:
                return col
        
        # Smart semantic matches for common patterns
        semantic_mappings = {
            # Time/Date patterns
            ("timestamp", "time", "date", "when"): ["order_date", "created_at", "updated_at", "date"],
            # Revenue/Money patterns  
            ("revenue", "sales", "money", "amount", "price", "cost", "value", "usd", "total"): 
                ["total_amount", "amount", "price", "cost", "revenue", "sales"],
            # Category patterns
            ("category", "type", "kind", "group", "segment"): 
                ["product_category", "category", "type", "segment"],
            # ID patterns
            ("id", "identifier", "key"): ["order_id", "customer_id", "product_id", "id"],
            # Method/Channel patterns
            ("method", "channel", "way", "mode"): ["payment_method", "shipping_method", "method"],
            # Status patterns
            ("status", "state", "condition"): ["order_status", "status", "state"]
        }
        
        for keywords, target_patterns in semantic_mappings.items():
            if any(keyword in target_lower for keyword in keywords):
                for pattern in target_patterns:
                    for col in available_columns:
                        if pattern.lower() in col.lower():
                            return col
                            
        # Fallback: fuzzy matching by similarity
        best_match = None
        best_score = 0
        for col in available_columns:
            # Simple similarity score based on common characters
            common_chars = set(target_lower) & set(col.lower())
            similarity = len(common_chars) / max(len(target_lower), len(col.lower()))
            if similarity > best_score and similarity > 0.3:  # At least 30% similarity
                best_score = similarity
                best_match = col
                
        return best_match

    async def select_kpis(
        self, domain: str, profile_summary: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Select appropriate KPIs for the given domain and data.

        Args:
            domain: Detected business domain
            profile_summary: Summary of the data profile

        Returns:
            KPI selection result
        """
        system_prompt = f"""You are a business intelligence expert specializing in {domain} analytics.
Your task is to select the most relevant KPIs (Key Performance Indicators) for this dataset.

Guidelines:
1. Select 1-3 primary KPIs that are most important for {domain} businesses
2. Ensure the selected KPIs can be calculated from available data
3. Prioritize KPIs that provide actionable business insights
4. Consider the data quality and completeness

Respond with valid JSON format."""

        user_prompt = f"""
Domain: {domain}

Available Data:
- Total rows: {profile_summary.get('total_rows', 0):,}
- Total columns: {profile_summary.get('total_columns', 0)}

Available Columns:
"""

        # Add column information
        for col in profile_summary.get("columns", [])[:15]:
            user_prompt += f"\n- '{col['name']}' ({col['type']})"
            if col.get("sample_values"):
                sample_str = ", ".join(str(v) for v in col["sample_values"][:3])
                user_prompt += f" - Sample: {sample_str}"

        user_prompt += f"""

Numeric Columns: {', '.join(profile_summary.get('numeric_columns', []))}
Categorical Columns: {', '.join(profile_summary.get('categorical_columns', []))}
DateTime Columns: {', '.join(profile_summary.get('datetime_columns', []))}

Please select the most appropriate KPIs and specify how to calculate them.

Respond in JSON format:
{{
    "selected_kpis": [
        {{
            "name": "KPI Name",
            "description": "What this KPI measures",
            "calculation": "How to calculate it from the data",
            "columns_needed": ["column1", "column2"],
            "importance": "high|medium|low",
            "reasoning": "Why this KPI is important for {domain}"
        }}
    ],
    "reasoning": "Overall explanation for KPI selection"
}}
"""

        try:
            response = await self._make_llm_request(
                system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.3
            )

            result = json.loads(response)
            return result

        except Exception as e:
            logger.error(f"KPI selection failed: {e}")
            raise LLMException(f"KPI selection failed: {str(e)}")

    async def select_charts(
        self, domain: str, profile_summary: Dict[str, Any], kpis: List[Dict]
    ) -> Dict[str, Any]:
        """
        Select appropriate chart types for the given domain and data.

        Args:
            domain: Business domain
            profile_summary: Data profile summary
            kpis: Selected KPIs

        Returns:
            Chart selection result
        """
        system_prompt = f"""You are a data visualization expert specializing in {domain} dashboards.
Your task is to select the most effective chart types for displaying this data.

Guidelines:
1. Choose 3-6 charts that best represent the data and support business decisions
2. Consider data types, relationships, and typical {domain} visualization needs
3. Include time-series charts if datetime data is available
4. Balance different chart types for comprehensive insights
5. Ensure charts are actionable and relevant for {domain} stakeholders

Chart types available: line, bar, pie, scatter, histogram, heatmap, funnel, gauge"""

        user_prompt = f"""
Domain: {domain}
Data Summary: {json.dumps(profile_summary, indent=2)}

Selected KPIs:
"""

        for kpi in kpis:
            user_prompt += f"- {kpi.get('name', 'Unknown')}: {kpi.get('description', 'No description')}\n"

        user_prompt += """
Please select the most appropriate charts for this dashboard.

Respond in JSON format:
{
    "selected_charts": [
        {
            "type": "line|bar|pie|scatter|histogram|heatmap|funnel|gauge",
            "title": "Chart Title",
            "description": "What this chart shows",
            "x_axis": "column_name or null",
            "y_axis": "column_name or null", 
            "color_by": "column_name or null",
            "aggregation": "sum|avg|count|max|min or null",
            "filters": ["column1", "column2"] or [],
            "importance": "high|medium|low",
            "reasoning": "Why this chart is valuable"
        }
    ],
    "layout_suggestions": {
        "primary_charts": ["chart1", "chart2"],
        "secondary_charts": ["chart3", "chart4"],
        "layout_priority": "time_series_first|kpis_first|distribution_first"
    }
}
"""

        try:
            response = await self._make_llm_request(
                system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.4
            )

            result = json.loads(response)
            return result

        except Exception as e:
            logger.error(f"Chart selection failed: {e}")
            raise LLMException(f"Chart selection failed: {str(e)}")

    async def parse_natural_language_query(
        self, query: str, available_columns: List[str], domain: str
    ) -> Dict[str, Any]:
        """
        Parse a natural language query into actionable chart/analysis instructions.

        Args:
            query: Natural language query from user
            available_columns: List of available column names
            domain: Business domain context

        Returns:
            Parsed query with execution plan
        """
        system_prompt = f"""You are a data analysis assistant for {domain} data.
Your task is to interpret natural language queries and convert them into specific data analysis instructions.

CRITICAL: You must ONLY use columns that exist in the available columns list. Never invent or assume column names.

Available columns: {', '.join(available_columns)}

Guidelines:
1. Understand the user's intent and desired visualization
2. Map user requests ONLY to available columns (never make up column names)
3. If the user asks for a column that doesn't exist, find the closest match from available columns
4. Suggest appropriate chart types and configurations
5. Provide clear execution steps
6. For count aggregations, use the appropriate column for counting

IMPORTANT COLUMN MAPPING RULES:
- For "sales channel" or "channel", look for columns like: payment_method, order_status, shipping_country
- For "timestamp" or "time", look for columns with "date" in the name
- For "count" aggregations, specify the column to count (usually an ID column)
- For time-series data with dates, consider suggesting monthly aggregation to avoid crowded charts
- Never use column names not in the available list"""

        user_prompt = f"""
User Query: "{query}"

Domain Context: {domain}
Available Columns: {available_columns}

CRITICAL INSTRUCTIONS:
1. ONLY use column names from the available columns list above
2. If the user mentions "sales channel" or "channel", map it to "payment_method" (the closest available column)
3. If the user mentions "timestamp" or time-related data, use "order_date" 
4. For counting orders, use "order_id" as the column to count
5. Validate that ALL column names in your response exist in the available columns list

Please analyze this query and provide an execution plan.

Important: Use only single column names for x_axis and y_axis, not arrays or complex expressions.

Respond in JSON format:
{{
    "intent": "visualization|analysis|filter|summary",
    "chart_type": "line|bar|pie|scatter|histogram|heatmap|table",
    "chart_config": {{
        "title": "Generated chart title",
        "x_axis": "single_column_name_from_available_list or null",
        "y_axis": "single_column_name_from_available_list or null",
        "color_by": "single_column_name_from_available_list or null",
        "aggregation": "sum|avg|count|max|min|none",
        "filters": {{}}
    }},
    "execution_steps": [
        "Step 1: Load data",
        "Step 2: Apply filters", 
        "Step 3: Create visualization"
    ],
    "column_mapping": {{
        "user_mentioned": "what user said",
        "mapped_to": "actual_column_name_used",
        "reason": "why this mapping was chosen"
    }},
    "confidence": 0.85,
    "reasoning": "Explanation of interpretation and column choices"
}}
"""

        try:
            response = await self._make_llm_request(
                system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.3
            )

            result = json.loads(response)
            return result

        except Exception as e:
            logger.error(f"NL query parsing failed: {e}")
            raise LLMException(f"Natural language parsing failed: {str(e)}")

    async def parse_chart_modification(
        self, 
        modification_query: str, 
        existing_chart: Dict[str, Any], 
        available_columns: List[str], 
        domain: str
    ) -> Dict[str, Any]:
        """
        Parse a natural language request to modify an existing chart.

        Args:
            modification_query: Natural language modification request
            existing_chart: Current chart configuration
            available_columns: List of available column names
            domain: Business domain context

        Returns:
            Modification plan with updated chart configuration
        """
        system_prompt = f"""You are a data visualization expert specializing in {domain} analytics.
Your task is to interpret natural language requests to modify existing charts.

Available columns: {', '.join(available_columns)}

Guidelines:
1. Understand what modifications the user wants to make
2. Preserve existing chart properties that aren't being changed
3. Map new requirements to available columns
4. Ensure the modified chart makes sense for the data
5. Provide clear explanations of what changes will be applied

Common modification types:
- Add/remove/change data series
- Change chart type
- Add/remove filters
- Change aggregation methods
- Modify grouping or color coding
- Adjust axes or scaling"""

        user_prompt = f"""
Modification Request: "{modification_query}"

Current Chart Configuration:
{json.dumps(existing_chart, indent=2)}

Domain Context: {domain}
Available Columns: {available_columns}

IMPORTANT: Only use column names that exist in the available columns list above. Do not make up column names.

Please analyze this modification request and provide a detailed plan.

Guidelines for column mapping:
- If the user asks for "category" but it doesn't exist, look for similar columns like "product_category", "type", etc.
- For count operations, you don't need a specific y_axis column - use aggregation: "count"
- Always validate that column names exist in the available columns list

Respond in JSON format:
{{
    "modification_type": "add_series|remove_series|change_chart_type|change_aggregation|add_filter|change_axes|other",
    "intent": "Clear description of what user wants to do",
    "feasible": true,
    "original_chart": {{
        "type": "current chart type",
        "title": "current title",
        "x_axis": "current x axis",
        "y_axis": "current y axis",
        "other_properties": "..."
    }},
    "new_chart_config": {{
        "id": "preserve_existing_id",
        "type": "updated chart type",
        "title": "updated title",
        "description": "updated description",
        "x_axis": "column_name_from_available_list_or_null",
        "y_axis": "column_name_from_available_list_or_null_for_count", 
        "color_by": "column_name_from_available_list_or_null",
        "aggregation": "sum|avg|count|max|min|none",
        "filters": ["updated filter list"],
        "sort_order": "asc|desc",
        "width": 6,
        "height": 4,
        "importance": "high|medium|low",
        "explanation": "Explanation of the modified chart"
    }},
    "changes_applied": [
        "List of specific changes made",
        "e.g., Changed chart type from bar to line",
        "e.g., Mapped category to closest available column"
    ],
    "sql_impact": "Description of how this affects the underlying query",
    "warnings": ["Any potential issues with the modification"],
    "confidence": 0.85,
    "reasoning": "Detailed explanation of the modification plan"
}}
"""

        try:
            response = await self._make_llm_request(
                system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.3
            )

            result = json.loads(response)
            
            # Ensure the new chart config preserves the original ID
            if "new_chart_config" in result and "id" in existing_chart:
                result["new_chart_config"]["id"] = existing_chart["id"]
            
            return result

        except Exception as e:
            logger.error(f"Chart modification parsing failed: {e}")
            raise LLMException(f"Chart modification parsing failed: {str(e)}")

    async def generate_chart_from_description(
        self, 
        description: str, 
        available_columns: List[str], 
        domain: str,
        existing_charts: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate a new chart configuration from a natural language description.

        Args:
            description: Natural language description of desired chart
            available_columns: List of available column names
            domain: Business domain context
            existing_charts: List of existing charts to avoid duplication

        Returns:
            Complete chart configuration
        """
        system_prompt = f"""You are a data visualization expert for {domain} analytics.
Your task is to create new chart configurations from natural language descriptions.

Available columns: {', '.join(available_columns)}

Guidelines:
1. Choose the most appropriate chart type for the data and request
2. Map description requirements to available columns intelligently
3. Create meaningful titles and descriptions
4. Consider business context for {domain} domain
5. Ensure the chart configuration is complete and valid
6. Avoid duplicating existing charts if provided"""

        existing_charts_info = ""
        if existing_charts:
            existing_charts_info = f"""
Existing Charts (avoid duplication):
{json.dumps([{
    'title': chart.get('title', ''),
    'type': chart.get('type', ''),
    'x_axis': chart.get('x_axis', ''),
    'y_axis': chart.get('y_axis', '')
} for chart in existing_charts], indent=2)}
"""

        user_prompt = f"""
Chart Description: "{description}"

Domain Context: {domain}
Available Columns: {available_columns}
{existing_charts_info}

Please create a complete chart configuration.

Respond in JSON format:
{{
    "feasible": true,
    "chart_config": {{
        "type": "line|bar|pie|scatter|histogram|heatmap|funnel|gauge|table",
        "title": "Descriptive chart title",
        "description": "What this chart shows and why it's useful",
        "x_axis": "column_name or null",
        "y_axis": "column_name or null",
        "color_by": "column_name or null",
        "size_by": "column_name or null",
        "aggregation": "sum|avg|count|max|min or null",
        "filters": [],
        "sort_by": "column_name or null",
        "sort_order": "asc|desc",
        "limit": 100,
        "width": 6,
        "height": 4,
        "importance": "high|medium|low",
        "explanation": "Business value and insights this chart provides"
    }},
    "sql_requirements": "Description of data requirements",
    "business_value": "Why this chart is valuable for {domain}",
    "confidence": 0.85,
    "reasoning": "Explanation of design choices"
}}
"""

        try:
            response = await self._make_llm_request(
                system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.3
            )

            result = json.loads(response)
            return result

        except Exception as e:
            logger.error(f"Chart generation failed: {e}")
            raise LLMException(f"Chart generation failed: {str(e)}")

    async def suggest_chart_improvements(
        self, 
        chart_config: Dict[str, Any], 
        data_sample: Dict[str, Any],
        available_columns: List[str], 
        domain: str
    ) -> Dict[str, Any]:
        """
        Suggest improvements to an existing chart based on data analysis.

        Args:
            chart_config: Current chart configuration
            data_sample: Sample of the actual data
            available_columns: List of available column names
            domain: Business domain context

        Returns:
            Improvement suggestions
        """
        system_prompt = f"""You are a data visualization consultant for {domain} analytics.
Your task is to analyze existing charts and suggest improvements for better insights.

Guidelines:
1. Consider data distribution and patterns
2. Suggest better chart types if appropriate
3. Recommend additional dimensions or filters
4. Identify potential data quality issues
5. Focus on business value for {domain} context"""

        user_prompt = f"""
Current Chart Configuration:
{json.dumps(chart_config, indent=2)}

Data Sample (first few rows):
{json.dumps(data_sample, indent=2)}

Domain Context: {domain}
Available Columns: {available_columns}

Please analyze this chart and suggest improvements.

Respond in JSON format:
{{
    "overall_assessment": "Brief assessment of current chart effectiveness",
    "improvements": [
        {{
            "type": "chart_type|data_dimension|filtering|aggregation|formatting",
            "current": "What it currently does",
            "suggested": "What it should do instead", 
            "reasoning": "Why this improvement helps",
            "impact": "high|medium|low"
        }}
    ],
    "alternative_charts": [
        {{
            "type": "different chart type",
            "description": "What this alternative would show",
            "use_case": "When this would be better"
        }}
    ],
    "data_quality_notes": ["Any data quality observations"],
    "business_insights": ["Key business insights this chart could reveal"],
    "confidence": 0.85
}}
"""

        try:
            response = await self._make_llm_request(
                system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.3
            )

            result = json.loads(response)
            return result

        except Exception as e:
            logger.error(f"Chart improvement analysis failed: {e}")
            raise LLMException(f"Chart improvement analysis failed: {str(e)}")

    async def _make_llm_request_with_reasoning(
        self,
        user_prompt: str,
        system_prompt: Optional[str] = None,
        use_reasoning: bool = False,
        temperature: float = 0.7,
    ) -> str:
        """
        Make LLM request with optional reasoning model for complex tasks.

        Args:
            user_prompt: The user prompt
            system_prompt: System prompt (ignored for o1 models)
                         use_reasoning: Whether to use gpt-4o-mini for complex reasoning
            temperature: Temperature for sampling

        Returns:
            LLM response text
        """
        try:
            model_to_use = self.reasoning_model if use_reasoning else self.model

            if use_reasoning:
                # o1 models don't support system messages or temperature
                messages = [{"role": "user", "content": user_prompt}]
                completion = await self.client.chat.completions.create(
                    model=model_to_use,
                    messages=messages,
                )
            else:
                # Regular GPT-4 model with system message and temperature
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": user_prompt})

                completion = await self.client.chat.completions.create(
                    model=model_to_use,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=4000,
                )

            response = completion.choices[0].message.content
            logger.info(f"LLM request successful with model: {model_to_use}")
            return response

        except Exception as e:
            logger.error(f"LLM request failed: {e}")
            raise LLMException(f"Failed to get LLM response: {str(e)}")

    async def _make_llm_request(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7,
    ) -> str:
        """
        Make a standard LLM request.

        Args:
            system_prompt: System instruction
            user_prompt: User query
            temperature: Sampling temperature

        Returns:
            LLM response
        """
        return await self._make_llm_request_with_reasoning(
            user_prompt=user_prompt,
            system_prompt=system_prompt,
            use_reasoning=False,
            temperature=temperature,
        )

    async def estimate_cost(self, prompt_tokens: int, completion_tokens: int) -> float:
        """
        Estimate the cost of an LLM request.

        Args:
            prompt_tokens: Number of input tokens
            completion_tokens: Number of output tokens

        Returns:
            Estimated cost in USD
        """
        input_cost_per_1k = 0.00015  # $0.00015 per 1K input tokens
        output_cost_per_1k = 0.0006  # $0.0006 per 1K output tokens

        input_cost = (prompt_tokens / 1000) * input_cost_per_1k
        output_cost = (completion_tokens / 1000) * output_cost_per_1k

        return input_cost + output_cost

    async def generate_structured_response(
        self,
        prompt: str,
        response_model: Type[T],
        temperature: float = 0.3,
        system_prompt: Optional[str] = None,
    ) -> T:
        """
        Generate structured response using OpenAI with response format.

        Args:
            prompt: User prompt
            response_model: Pydantic model for structured response
            temperature: Sampling temperature
            system_prompt: Optional system prompt

        Returns:
            Structured response matching the response_model
        """
        if system_prompt is None:
            system_prompt = "You are a helpful AI assistant that provides accurate, structured responses."

        try:
            # Get the JSON schema from the Pydantic model
            schema = response_model.model_json_schema()

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
                temperature=temperature,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": response_model.__name__,
                        "schema": schema,
                        "strict": True,
                    },
                },
            )

            content = response.choices[0].message.content
            if not content:
                raise LLMException("Empty response from OpenAI")

            # Parse and validate the response
            data = json.loads(content)
            return response_model.model_validate(data)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise LLMException(f"Invalid JSON response: {e}")
        except Exception as e:
            logger.error(f"Error generating structured response: {e}")
            raise LLMException(f"Failed to generate structured response: {e}")


# Global client instance
llm_client = LLMClient()
