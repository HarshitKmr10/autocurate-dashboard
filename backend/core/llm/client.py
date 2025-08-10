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
            logger.error(f"Domain classification failed: {e}")
            raise LLMException(f"Domain classification failed: {str(e)}")

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

Available columns: {', '.join(available_columns)}

Guidelines:
1. Understand the user's intent and desired visualization
2. Map user requests to available columns
3. Suggest appropriate chart types and configurations
4. Provide clear execution steps"""

        user_prompt = f"""
User Query: "{query}"

Domain Context: {domain}
Available Columns: {available_columns}

Please analyze this query and provide an execution plan.

Respond in JSON format:
{{
    "intent": "visualization|analysis|filter|summary",
    "chart_type": "line|bar|pie|scatter|histogram|heatmap|table",
    "chart_config": {{
        "title": "Generated chart title",
        "x_axis": "column_name or null",
        "y_axis": "column_name or null",
        "color_by": "column_name or null",
        "aggregation": "sum|avg|count|max|min or null",
        "filters": {{}}
    }},
    "execution_steps": [
        "Step 1: Load data",
        "Step 2: Apply filters", 
        "Step 3: Create visualization"
    ],
    "confidence": 0.85,
    "reasoning": "Explanation of interpretation"
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
