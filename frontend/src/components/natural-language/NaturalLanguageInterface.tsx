/**
 * Natural Language Interface for creating and modifying charts
 */

"use client";

import React, { useState, useRef, useEffect } from 'react';
import { 
  MessageSquare, 
  Send, 
  Loader2, 
  CheckCircle, 
  AlertCircle, 
  Plus,
  Edit3,
  Eye,
  Trash2,
  Lightbulb
} from 'lucide-react';
import { 
  parseNaturalLanguageQuery, 
  executeNaturalLanguageQuery,
  modifyChart,
  applyChartModification,
  addChartToDashboard
} from '@/lib/api';
import { BaseChart } from '@/components/charts/BaseChart';
import { 
  NaturalLanguageResponse, 
  ExecutionResponse, 
  ModificationResponse,
  ModificationApplicationResponse,
  ChartConfig,
  QueryResult,
  ChartModificationPlan,
  NaturalLanguageQuery
} from '@/types';

interface NaturalLanguageInterfaceProps {
  datasetId: string;
  onChartCreated?: (chartConfig: ChartConfig, data: QueryResult) => void;
  onChartModified?: (originalChart: ChartConfig, newChart: ChartConfig, data: QueryResult) => void;
  existingChart?: ChartConfig;
  mode?: 'create' | 'modify';
  onClose?: () => void;
}

interface ConversationMessage {
  id: string;
  type: 'user' | 'system' | 'preview' | 'plan';
  content: string;
  timestamp: Date;
  data?: any;
}

export function NaturalLanguageInterface({
  datasetId,
  onChartCreated,
  onChartModified,
  existingChart,
  mode = 'create',
  onClose
}: NaturalLanguageInterfaceProps) {
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [conversation, setConversation] = useState<ConversationMessage[]>([]);
  const [pendingPlan, setPendingPlan] = useState<ChartModificationPlan | null>(null);
  const [previewChart, setPreviewChart] = useState<{ config: ChartConfig; data: QueryResult } | null>(null);
  const [error, setError] = useState<string | null>(null);
  
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [conversation]);

  useEffect(() => {
    // Add initial message based on mode
    const initialMessage: ConversationMessage = {
      id: Date.now().toString(),
      type: 'system',
      content: mode === 'create' 
        ? "Hi! I can help you create charts using natural language. Try asking for things like:\n\n• \"Show revenue by month as a line chart\"\n• \"Create a bar chart of sales by region\"\n• \"Histogram of customer age with 20 bins\"\n• \"Pie chart showing product category distribution\""
        : `I can help you modify the "${existingChart?.title}" chart. Try asking for things like:\n\n• \"Add revenue to this chart\"\n• \"Change this to a line chart\"\n• \"Group by customer segment\"\n• \"Add a filter for region\"`,
      timestamp: new Date()
    };
    setConversation([initialMessage]);
  }, [mode, existingChart]);

  const addMessage = (type: ConversationMessage['type'], content: string, data?: any) => {
    const message: ConversationMessage = {
      id: Date.now().toString(),
      type,
      content,
      timestamp: new Date(),
      data
    };
    setConversation(prev => [...prev, message]);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userQuery = input.trim();
    setInput('');
    setError(null);
    
    addMessage('user', userQuery);
    setIsLoading(true);

    try {
      if (mode === 'create') {
        await handleCreateChart(userQuery);
      } else {
        await handleModifyChart(userQuery);
      }
    } catch (error: any) {
      setError(error.message || 'An error occurred');
      addMessage('system', `Sorry, I encountered an error: ${error.message || 'Unknown error'}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleCreateChart = async (query: string) => {
    try {
      // Parse the natural language query
      addMessage('system', 'Understanding your request...');
      const parseResponse: NaturalLanguageResponse = await parseNaturalLanguageQuery(datasetId, query);
      
      const { parsed_query } = parseResponse;
      
      // Check if the LLM used correct columns by validating against available columns
      const availableColumns = parseResponse.available_columns || [];
      const { x_axis, y_axis, color_by } = parsed_query.chart_config;
      
      // Validate column references
      const invalidColumns = [];
      if (x_axis && !availableColumns.includes(x_axis)) invalidColumns.push(`x_axis: ${x_axis}`);
      if (y_axis && !availableColumns.includes(y_axis)) invalidColumns.push(`y_axis: ${y_axis}`);
      if (color_by && !availableColumns.includes(color_by)) invalidColumns.push(`color_by: ${color_by}`);
      
      if (invalidColumns.length > 0) {
        addMessage('system', `⚠️ Warning: The chart plan references columns that don't exist in your dataset:\n\n${invalidColumns.join('\n')}\n\nAvailable columns: ${availableColumns.join(', ')}\n\nThis may result in a blank chart. Please try rephrasing your request using the available column names.`);
        return;
      }
      
      // Show the parsed plan
      const planContent = `I understand you want to create a ${parsed_query.chart_type} chart. Here's my plan:

**Chart Type:** ${parsed_query.chart_type.toUpperCase()}
**Title:** ${parsed_query.chart_config.title}
**X-Axis:** ${parsed_query.chart_config.x_axis || 'Not specified'}
**Y-Axis:** ${parsed_query.chart_config.y_axis || 'Not specified'}
**Aggregation:** ${parsed_query.chart_config.aggregation || 'None'}
**Confidence:** ${(parsed_query.confidence * 100).toFixed(1)}%

**Reasoning:** ${parsed_query.reasoning}

Would you like me to create this chart?`;

      addMessage('plan', planContent, { parsedQuery: parsed_query, action: 'create' });
      
    } catch (error: any) {
      // Handle specific error cases
      if (error.message?.includes('Dataset not found')) {
        throw new Error('This dataset is not available or has been deleted. Please refresh the page or upload a new dataset.');
      } else if (error.message?.includes('Failed to parse query')) {
        throw new Error('Unable to understand your request. Please try rephrasing it more simply, for example: "Show sales by month as a line chart"');
      }
      throw new Error(`Failed to parse query: ${error.message}`);
    }
  };

  const handleModifyChart = async (query: string) => {
    if (!existingChart) {
      throw new Error('No existing chart to modify');
    }

    try {
      // Parse the modification request
      addMessage('system', 'Analyzing modification request...');
      const modifyResponse: ModificationResponse = await modifyChart(datasetId, query, existingChart);
      
      const { modification_plan } = modifyResponse;
      
      if (!modification_plan.feasible) {
        addMessage('system', `I'm sorry, but this modification isn't feasible: ${modification_plan.reasoning}`);
        return;
      }

      // Show the modification plan
      const planContent = `I can make the following changes to your chart:

**Modification Type:** ${modification_plan.modification_type.replace('_', ' ').toUpperCase()}
**Changes:**
${modification_plan.changes_applied.map(change => `• ${change}`).join('\n')}

**Impact:** ${modification_plan.sql_impact}

${modification_plan.warnings.length > 0 ? `**Warnings:**\n${modification_plan.warnings.map(w => `⚠️ ${w}`).join('\n')}` : ''}

**Confidence:** ${(modification_plan.confidence * 100).toFixed(1)}%

Would you like me to apply these changes?`;

      addMessage('plan', planContent, { modificationPlan: modification_plan, action: 'modify' });
      setPendingPlan(modification_plan);
      
    } catch (error: any) {
      throw new Error(`Failed to parse modification: ${error.message}`);
    }
  };

  const handleApproveAction = async (messageData: any) => {
    setIsLoading(true);
    try {
      if (messageData.action === 'create') {
        await executeCreateChart(messageData.parsedQuery);
      } else if (messageData.action === 'modify') {
        await executeModifyChart(messageData.modificationPlan);
      }
    } catch (error: any) {
      setError(error.message || 'An error occurred');
      addMessage('system', `Error executing action: ${error.message || 'Unknown error'}`);
    } finally {
      setIsLoading(false);
    }
  };

  const executeCreateChart = async (parsedQuery: NaturalLanguageQuery) => {
    try {
      addMessage('system', 'Creating your chart...');
      
      // Execute the parsed query
      const executeResponse: ExecutionResponse = await executeNaturalLanguageQuery(datasetId, parsedQuery);
      
      setPreviewChart({
        config: executeResponse.chart_config,
        data: executeResponse.data
      });

      addMessage('preview', 'Here\'s your new chart:', {
        chartConfig: executeResponse.chart_config,
        chartData: executeResponse.data,
        sqlQuery: executeResponse.sql_query
      });

      addMessage('system', 'Chart created successfully! You can add it to your dashboard or make further modifications.');
      
    } catch (error: any) {
      let errorMessage = error.message || 'Unknown error occurred';
      
      // Handle specific error types
      if (errorMessage.includes('Dataset not properly loaded')) {
        errorMessage = 'The dataset needs to be reloaded. Please refresh the page and try again.';
      } else if (errorMessage.includes('columns were not found')) {
        errorMessage = 'Some columns referenced in your request don\'t exist in the data. Please check the column names or try a different description.';
      } else if (errorMessage.includes('syntax issues')) {
        errorMessage = 'Your request couldn\'t be converted to a valid query. Please try rephrasing it more simply.';
      }
      
      throw new Error(errorMessage);
    }
  };

  const executeModifyChart = async (modificationPlan: ChartModificationPlan) => {
    try {
      addMessage('system', 'Applying modifications...');
      
      // Apply the modification
      const applyResponse: ModificationApplicationResponse = await applyChartModification(datasetId, modificationPlan);
      
      setPreviewChart({
        config: applyResponse.new_chart_config,
        data: applyResponse.data
      });

      addMessage('preview', 'Here\'s your modified chart:', {
        chartConfig: applyResponse.new_chart_config,
        chartData: applyResponse.data,
        sqlQuery: applyResponse.sql_query,
        changesApplied: applyResponse.changes_applied
      });

      addMessage('system', 'Chart modified successfully! The changes have been applied.');
      
      // Notify parent component
      if (onChartModified && existingChart) {
        onChartModified(existingChart, applyResponse.new_chart_config, applyResponse.data);
      }
      
    } catch (error: any) {
      let errorMessage = error.message || 'Unknown error occurred';
      
      // Handle specific error types
      if (errorMessage.includes('Dataset not properly loaded')) {
        errorMessage = 'The dataset needs to be reloaded. Please refresh the page and try again.';
      } else if (errorMessage.includes('columns were not found')) {
        errorMessage = 'Some columns referenced in your request don\'t exist in the data. Please check the column names.';
      } else if (errorMessage.includes('syntax issues')) {
        errorMessage = 'Your request couldn\'t be converted to a valid query. Please try rephrasing it.';
      }
      
      throw new Error(errorMessage);
    }
  };

  const handleAddToDashboard = async () => {
    if (!previewChart) return;

    setIsLoading(true);
    try {
      await addChartToDashboard(datasetId, previewChart.config);
      addMessage('system', '✅ Chart added to dashboard successfully!');
      
      // Notify parent component
      if (onChartCreated) {
        onChartCreated(previewChart.config, previewChart.data);
      }
      
      // Close the interface after a brief delay
      setTimeout(() => {
        onClose?.();
      }, 2000);
      
    } catch (error: any) {
      setError(error.message || 'Failed to add chart to dashboard');
      addMessage('system', `Failed to add chart to dashboard: ${error.message || 'Unknown error'}`);
    } finally {
      setIsLoading(false);
    }
  };

  const renderMessage = (message: ConversationMessage) => {
    switch (message.type) {
      case 'user':
        return (
          <div className="flex justify-end mb-4">
            <div className="bg-blue-500 text-white rounded-lg px-4 py-2 max-w-3xl">
              {message.content}
            </div>
          </div>
        );
        
      case 'system':
        return (
          <div className="flex justify-start mb-4">
            <div className="bg-gray-100 text-gray-800 rounded-lg px-4 py-2 max-w-3xl">
              <div className="flex items-start gap-2">
                <MessageSquare className="w-4 h-4 mt-1 text-blue-500 flex-shrink-0" />
                <div className="whitespace-pre-wrap">{message.content}</div>
              </div>
            </div>
          </div>
        );
        
      case 'plan':
        return (
          <div className="flex justify-start mb-4">
            <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-3 max-w-3xl">
              <div className="flex items-start gap-2 mb-3">
                <Lightbulb className="w-4 h-4 mt-1 text-yellow-600 flex-shrink-0" />
                <div className="whitespace-pre-wrap text-gray-800">{message.content}</div>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => handleApproveAction(message.data)}
                  disabled={isLoading}
                  className="bg-green-500 hover:bg-green-600 text-white px-3 py-1 rounded text-sm flex items-center gap-1 disabled:opacity-50"
                >
                  <CheckCircle className="w-3 h-3" />
                  {message.data?.action === 'create' ? 'Create Chart' : 'Apply Changes'}
                </button>
                <button
                  onClick={() => addMessage('system', 'Action cancelled. Please describe what you\'d like differently.')}
                  disabled={isLoading}
                  className="bg-gray-500 hover:bg-gray-600 text-white px-3 py-1 rounded text-sm"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        );
        
      case 'preview':
        return (
          <div className="flex justify-start mb-4">
            <div className="bg-white border border-gray-200 rounded-lg p-4 max-w-4xl w-full">
              <div className="flex items-center gap-2 mb-3">
                <Eye className="w-4 h-4 text-blue-500" />
                <span className="font-medium text-gray-800">Chart Preview</span>
              </div>
              
              {message.data?.chartConfig && message.data?.chartData && (
                <div className="mb-4">
                  <BaseChart
                    config={message.data.chartConfig}
                    data={message.data.chartData}
                    width={500}
                    height={300}
                  />
                </div>
              )}
              
              {message.data?.changesApplied && (
                <div className="mb-3 p-3 bg-green-50 border border-green-200 rounded">
                  <h4 className="font-medium text-green-800 mb-2">Changes Applied:</h4>
                  <ul className="text-sm text-green-700">
                    {message.data.changesApplied.map((change: string, index: number) => (
                      <li key={index} className="flex items-start gap-1">
                        <CheckCircle className="w-3 h-3 mt-0.5 flex-shrink-0" />
                        {change}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              
              {mode === 'create' && (
                <div className="flex gap-2">
                  <button
                    onClick={handleAddToDashboard}
                    disabled={isLoading}
                    className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded flex items-center gap-2 disabled:opacity-50"
                  >
                    <Plus className="w-4 h-4" />
                    Add to Dashboard
                  </button>
                </div>
              )}
              
              {message.data?.sqlQuery && (
                <details className="mt-3">
                  <summary className="cursor-pointer text-sm text-gray-600 hover:text-gray-800">
                    View SQL Query
                  </summary>
                  <pre className="mt-2 p-2 bg-gray-100 rounded text-xs overflow-x-auto">
                    {message.data.sqlQuery}
                  </pre>
                </details>
              )}
            </div>
          </div>
        );
        
      default:
        return null;
    }
  };

  return (
    <div className="flex flex-col h-full bg-white">
      {/* Header */}
      <div className="border-b border-gray-200 p-4">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">
              {mode === 'create' ? 'Create Chart' : 'Modify Chart'} with Natural Language
            </h3>
            <p className="text-sm text-gray-600">
              {mode === 'create' 
                ? 'Describe the chart you want to create in plain English'
                : `Modify "${existingChart?.title}" using natural language`
              }
            </p>
          </div>
          {onClose && (
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600"
            >
              <Trash2 className="w-5 h-5" />
            </button>
          )}
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {conversation.map(message => (
          <div key={message.id}>
            {renderMessage(message)}
          </div>
        ))}
        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-gray-100 rounded-lg px-4 py-2">
              <div className="flex items-center gap-2">
                <Loader2 className="w-4 h-4 animate-spin text-blue-500" />
                <span className="text-gray-600">Processing...</span>
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Error Display */}
      {error && (
        <div className="mx-4 mb-4 p-3 bg-red-50 border border-red-200 rounded-lg">
          <div className="flex items-start gap-2">
            <AlertCircle className="w-4 h-4 text-red-500 mt-0.5 flex-shrink-0" />
            <span className="text-red-700 text-sm">{error}</span>
          </div>
        </div>
      )}

      {/* Input Form */}
      <div className="border-t border-gray-200 p-4">
        <form onSubmit={handleSubmit} className="flex gap-2">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={mode === 'create' 
              ? "e.g., 'Show sales by month as a line chart' or 'Create a pie chart of customer segments'"
              : "e.g., 'Add revenue to this chart' or 'Change this to a line chart'"
            }
            className="flex-1 resize-none border border-gray-300 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            rows={3}
            disabled={isLoading}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                handleSubmit(e);
              }
            }}
          />
          <button
            type="submit"
            disabled={!input.trim() || isLoading}
            className="self-end bg-blue-500 hover:bg-blue-600 disabled:bg-gray-300 text-white p-2 rounded-lg transition-colors"
          >
            {isLoading ? (
              <Loader2 className="w-5 h-5 animate-spin" />
            ) : (
              <Send className="w-5 h-5" />
            )}
          </button>
        </form>
        
        {/* Example queries */}
        <div className="mt-3 flex flex-wrap gap-2">
          {mode === 'create' ? (
            <>
              <button
                onClick={() => setInput('Show revenue by month as a line chart')}
                className="text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 px-2 py-1 rounded"
                disabled={isLoading}
              >
                Revenue by month
              </button>
              <button
                onClick={() => setInput('Create a bar chart of orders by payment method')}
                className="text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 px-2 py-1 rounded"
                disabled={isLoading}
              >
                Orders by payment method
              </button>
              <button
                onClick={() => setInput('Pie chart showing product category distribution')}
                className="text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 px-2 py-1 rounded"
                disabled={isLoading}
              >
                Product categories
              </button>
            </>
          ) : (
            <>
              <button
                onClick={() => setInput('Add revenue to this chart')}
                className="text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 px-2 py-1 rounded"
                disabled={isLoading}
              >
                Add revenue
              </button>
              <button
                onClick={() => setInput('Change this to a line chart')}
                className="text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 px-2 py-1 rounded"
                disabled={isLoading}
              >
                Change chart type
              </button>
              <button
                onClick={() => setInput('Group by customer segment')}
                className="text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 px-2 py-1 rounded"
                disabled={isLoading}
              >
                Group by segment
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
