"""Domain detection engine for identifying business context of datasets."""

import re
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from pydantic import BaseModel
from enum import Enum

from backend.core.profiler.data_profiler import DataProfile, ColumnProfile
from backend.core.llm.client import LLMClient
from backend.utils.exceptions import DomainDetectionException

logger = logging.getLogger(__name__)


class DomainType(str, Enum):
    """Enumeration of supported business domains."""
    ECOMMERCE = "ecommerce"
    FINANCE = "finance"
    MANUFACTURING = "manufacturing"
    SAAS = "saas"
    GENERIC = "generic"


class DomainClassification(BaseModel):
    """Result of domain classification."""
    domain: DomainType
    confidence: float
    reasoning: str
    rule_based_score: float
    llm_score: float
    detected_patterns: List[str]
    suggested_kpis: List[str]
    classified_at: datetime


class DomainDetector:
    """Main domain detection engine."""
    
    def __init__(self):
        self.llm_client = LLMClient()
        
        # Domain-specific keywords and patterns
        self.domain_patterns = {
            DomainType.ECOMMERCE: {
                'keywords': [
                    'order', 'product', 'customer', 'cart', 'purchase', 'payment',
                    'price', 'quantity', 'sku', 'inventory', 'shipping', 'discount',
                    'revenue', 'sales', 'buyer', 'seller', 'marketplace', 'catalog'
                ],
                'patterns': [
                    r'order[_\s]?(id|number|date)',
                    r'product[_\s]?(id|name|category)',
                    r'customer[_\s]?(id|name|email)',
                    r'unit[_\s]?price',
                    r'total[_\s]?(amount|price)',
                    r'payment[_\s]?(method|status)',
                    r'shipping[_\s]?(address|cost)'
                ]
            },
            DomainType.FINANCE: {
                'keywords': [
                    'account', 'transaction', 'balance', 'amount', 'currency',
                    'bank', 'credit', 'debit', 'investment', 'portfolio', 'risk',
                    'return', 'interest', 'loan', 'deposit', 'withdrawal', 'fee'
                ],
                'patterns': [
                    r'account[_\s]?(number|id|balance)',
                    r'transaction[_\s]?(id|amount|date|type)',
                    r'(credit|debit)[_\s]?amount',
                    r'interest[_\s]?rate',
                    r'(current|available)[_\s]?balance',
                    r'(currency|exchange)[_\s]?rate'
                ]
            },
            DomainType.MANUFACTURING: {
                'keywords': [
                    'production', 'machine', 'equipment', 'quality', 'defect',
                    'batch', 'lot', 'manufacturing', 'process', 'downtime',
                    'efficiency', 'yield', 'scrap', 'rework', 'oee', 'maintenance'
                ],
                'patterns': [
                    r'machine[_\s]?(id|name|status)',
                    r'production[_\s]?(line|date|quantity)',
                    r'quality[_\s]?(check|score|rating)',
                    r'batch[_\s]?(number|id|size)',
                    r'(downtime|uptime)[_\s]?hours',
                    r'(defect|error)[_\s]?(rate|count)'
                ]
            },
            DomainType.SAAS: {
                'keywords': [
                    'user', 'subscription', 'plan', 'usage', 'feature', 'session',
                    'login', 'signup', 'churn', 'retention', 'engagement', 'trial',
                    'conversion', 'mrr', 'arr', 'ltv', 'cac', 'dau', 'mau'
                ],
                'patterns': [
                    r'user[_\s]?(id|name|email|type)',
                    r'subscription[_\s]?(id|plan|status|date)',
                    r'(login|signin)[_\s]?(date|time|count)',
                    r'(trial|free)[_\s]?(start|end|period)',
                    r'(monthly|annual)[_\s]?(revenue|recurring)',
                    r'(churn|retention)[_\s]?rate'
                ]
            }
        }
    
    async def detect_domain(self, profile: DataProfile) -> DomainClassification:
        """
        Detect the business domain of a dataset.
        
        Args:
            profile: Data profile from the profiler
            
        Returns:
            DomainClassification with detected domain and confidence
        """
        logger.info(f"Starting domain detection for dataset {profile.dataset_id}")
        
        try:
            # Rule-based classification
            rule_results = await self._rule_based_classification(profile)
            
            # LLM-based classification
            llm_results = await self._llm_based_classification(profile)
            
            # Combine results
            final_classification = await self._combine_classifications(
                rule_results, llm_results, profile
            )
            
            logger.info(f"Domain detection completed: {final_classification.domain} "
                       f"(confidence: {final_classification.confidence:.2f})")
            
            return final_classification
            
        except Exception as e:
            logger.error(f"Domain detection failed: {e}", exc_info=True)
            raise DomainDetectionException(f"Failed to detect domain: {str(e)}")
    
    async def _rule_based_classification(self, profile: DataProfile) -> Dict[DomainType, float]:
        """
        Perform rule-based domain classification.
        
        Args:
            profile: Data profile
            
        Returns:
            Dictionary with domain scores
        """
        domain_scores = {domain: 0.0 for domain in DomainType}
        
        # Analyze column names
        all_column_names = [col.original_name.lower() for col in profile.columns]
        column_text = ' '.join(all_column_names)
        
        for domain_type, patterns in self.domain_patterns.items():
            score = 0.0
            
            # Keyword matching
            keyword_matches = 0
            for keyword in patterns['keywords']:
                if keyword in column_text:
                    keyword_matches += 1
            
            # Pattern matching
            pattern_matches = 0
            for pattern in patterns['patterns']:
                if re.search(pattern, column_text, re.IGNORECASE):
                    pattern_matches += 1
            
            # Calculate score based on matches
            keyword_score = min(keyword_matches / len(patterns['keywords']), 1.0) * 0.6
            pattern_score = min(pattern_matches / len(patterns['patterns']), 1.0) * 0.4
            
            domain_scores[domain_type] = keyword_score + pattern_score
        
        logger.debug(f"Rule-based scores: {domain_scores}")
        return domain_scores
    
    async def _llm_based_classification(self, profile: DataProfile) -> Dict[str, Any]:
        """
        Perform LLM-based domain classification.
        
        Args:
            profile: Data profile
            
        Returns:
            Dictionary with LLM classification results
        """
        try:
            # Prepare data summary for LLM
            data_summary = self._prepare_data_summary(profile)
            
            # Create classification prompt
            prompt = self._create_domain_classification_prompt(data_summary)
            
            # Get LLM response
            response = await self.llm_client.classify_domain(prompt)
            
            logger.debug(f"LLM classification result: {response}")
            return response
            
        except Exception as e:
            logger.warning(f"LLM classification failed: {e}")
            # Return default response if LLM fails
            return {
                'domain': 'generic',
                'confidence': 0.3,
                'reasoning': 'LLM classification unavailable, using fallback'
            }
    
    def _prepare_data_summary(self, profile: DataProfile) -> Dict[str, Any]:
        """
        Prepare a concise summary of the data for LLM analysis.
        
        Args:
            profile: Data profile
            
        Returns:
            Summary dictionary
        """
        # Select most informative columns
        sample_columns = []
        for col in profile.columns[:20]:  # Limit to first 20 columns
            col_info = {
                'name': col.original_name,
                'type': col.data_type,
                'sample_values': col.sample_values[:5],
                'unique_count': col.unique_count,
                'null_percentage': col.null_percentage
            }
            
            # Add top values for categorical columns
            if col.data_type == 'categorical' and col.top_values:
                col_info['top_values'] = [v['value'] for v in col.top_values[:3]]
            
            sample_columns.append(col_info)
        
        return {
            'total_rows': profile.total_rows,
            'total_columns': profile.total_columns,
            'columns': sample_columns,
            'has_datetime': profile.has_datetime,
            'has_numeric': profile.has_numeric,
            'numeric_columns': profile.numeric_columns[:10],
            'categorical_columns': profile.categorical_columns[:10],
            'datetime_columns': profile.datetime_columns,
            'potential_id_columns': profile.potential_id_columns
        }
    
    def _create_domain_classification_prompt(self, data_summary: Dict[str, Any]) -> str:
        """
        Create a prompt for LLM domain classification.
        
        Args:
            data_summary: Summary of the dataset
            
        Returns:
            Formatted prompt string
        """
        prompt = f"""
Analyze this dataset and classify it into one of these business domains:

1. **E-commerce**: Online retail, orders, products, customers, payments, shipping
2. **Finance**: Banking, transactions, accounts, investments, loans, payments
3. **Manufacturing**: Production, quality control, equipment, processes, defects
4. **SaaS**: Software usage, subscriptions, users, features, engagement, churn
5. **Generic**: Mixed or unclear domain that doesn't fit the above categories

Dataset Information:
- Total rows: {data_summary['total_rows']:,}
- Total columns: {data_summary['total_columns']}
- Has datetime data: {data_summary['has_datetime']}
- Has numeric data: {data_summary['has_numeric']}

Column Analysis:
"""
        
        # Add column information
        for i, col in enumerate(data_summary['columns'][:15]):
            prompt += f"\n{i+1}. '{col['name']}' ({col['type']})"
            if col['sample_values']:
                sample_str = ', '.join(str(v) for v in col['sample_values'])
                prompt += f" - Sample: {sample_str}"
        
        prompt += f"""

Key Columns:
- Numeric columns: {', '.join(data_summary['numeric_columns'])}
- Categorical columns: {', '.join(data_summary['categorical_columns'])}
- DateTime columns: {', '.join(data_summary['datetime_columns'])}
- Potential ID columns: {', '.join(data_summary['potential_id_columns'])}

Please analyze the column names, data types, and sample values to determine the most likely business domain.

Respond in JSON format:
{{
    "domain": "ecommerce|finance|manufacturing|saas|generic",
    "confidence": 0.85,
    "reasoning": "Detailed explanation of why this domain was selected...",
    "key_indicators": ["indicator1", "indicator2", "indicator3"],
    "suggested_kpis": ["kpi1", "kpi2", "kpi3"]
}}
"""
        
        return prompt
    
    async def _combine_classifications(
        self,
        rule_results: Dict[DomainType, float],
        llm_results: Dict[str, Any],
        profile: DataProfile
    ) -> DomainClassification:
        """
        Combine rule-based and LLM-based classifications.
        
        Args:
            rule_results: Rule-based classification scores
            llm_results: LLM classification results
            profile: Data profile
            
        Returns:
            Final domain classification
        """
        # Get the highest scoring domain from rules
        best_rule_domain = max(rule_results.items(), key=lambda x: x[1])
        rule_domain, rule_score = best_rule_domain
        
        # Get LLM domain and confidence
        llm_domain_str = llm_results.get('domain', 'generic').lower()
        llm_confidence = llm_results.get('confidence', 0.5)
        
        # Convert LLM domain string to enum
        try:
            llm_domain = DomainType(llm_domain_str)
        except ValueError:
            llm_domain = DomainType.GENERIC
            llm_confidence = 0.3
        
        # Combine scores with weights
        rule_weight = 0.4
        llm_weight = 0.6
        
        # If rule-based and LLM agree, boost confidence
        if rule_domain == llm_domain:
            final_domain = rule_domain
            final_confidence = min(rule_score * rule_weight + llm_confidence * llm_weight + 0.2, 1.0)
            reasoning = f"Both rule-based analysis and LLM classification agree on {final_domain.value} domain."
        else:
            # Choose based on higher confidence
            if rule_score > llm_confidence:
                final_domain = rule_domain
                final_confidence = rule_score * 0.8  # Slightly reduce confidence due to disagreement
                reasoning = f"Rule-based analysis suggests {rule_domain.value} (score: {rule_score:.2f}), " \
                           f"while LLM suggests {llm_domain.value} (confidence: {llm_confidence:.2f}). " \
                           f"Using rule-based result due to higher confidence."
            else:
                final_domain = llm_domain
                final_confidence = llm_confidence * 0.8
                reasoning = f"LLM analysis suggests {llm_domain.value} (confidence: {llm_confidence:.2f}), " \
                           f"while rules suggest {rule_domain.value} (score: {rule_score:.2f}). " \
                           f"Using LLM result due to higher confidence."
        
        # Extract additional information from LLM results
        detected_patterns = llm_results.get('key_indicators', [])
        suggested_kpis = llm_results.get('suggested_kpis', [])
        
        # Add reasoning from LLM if available
        if llm_results.get('reasoning'):
            reasoning += f" LLM reasoning: {llm_results['reasoning']}"
        
        return DomainClassification(
            domain=final_domain,
            confidence=final_confidence,
            reasoning=reasoning,
            rule_based_score=rule_score,
            llm_score=llm_confidence,
            detected_patterns=detected_patterns,
            suggested_kpis=suggested_kpis,
            classified_at=datetime.utcnow()
        )