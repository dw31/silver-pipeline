"""
Error Handling and Logging Utilities for Silver Pipeline
Provides comprehensive error handling, logging, and monitoring capabilities
"""

import logging
import traceback
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from functools import wraps
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType


class SilverPipelineLogger:
    """Enhanced logging for Silver Pipeline with structured output"""
    
    def __init__(self, name: str, log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Create formatter for structured logging
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        # Console handler
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
    def log_pipeline_event(self, event_type: str, source: str, lob: str, domain: str, 
                          stage: str, message: str, additional_data: Dict[str, Any] = None):
        """Log structured pipeline events"""
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'source': source,
            'lob': lob,
            'domain': domain,
            'stage': stage,
            'message': message
        }
        
        if additional_data:
            log_data.update(additional_data)
        
        log_message = f"Pipeline Event: {json.dumps(log_data)}"
        
        if event_type == 'ERROR':
            self.logger.error(log_message)
        elif event_type == 'WARNING':
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)
    
    def log_data_quality_issue(self, source: str, lob: str, domain: str, stage: str,
                              issue_type: str, field_name: str = None, 
                              issue_details: Dict[str, Any] = None):
        """Log data quality issues with detailed context"""
        quality_log = {
            'timestamp': datetime.now().isoformat(),
            'event_type': 'DATA_QUALITY_ISSUE',
            'source': source,
            'lob': lob,
            'domain': domain,
            'stage': stage,
            'issue_type': issue_type,
            'field_name': field_name,
            'details': issue_details or {}
        }
        
        self.logger.warning(f"Data Quality Issue: {json.dumps(quality_log)}")
    
    def log_performance_metrics(self, source: str, lob: str, domain: str, stage: str,
                               record_count: int, processing_time_seconds: float,
                               additional_metrics: Dict[str, Any] = None):
        """Log performance metrics"""
        metrics_log = {
            'timestamp': datetime.now().isoformat(),
            'event_type': 'PERFORMANCE_METRICS',
            'source': source,
            'lob': lob,
            'domain': domain,
            'stage': stage,
            'record_count': record_count,
            'processing_time_seconds': processing_time_seconds,
            'records_per_second': record_count / processing_time_seconds if processing_time_seconds > 0 else 0
        }
        
        if additional_metrics:
            metrics_log.update(additional_metrics)
        
        self.logger.info(f"Performance Metrics: {json.dumps(metrics_log)}")


class PipelineErrorHandler:
    """Centralized error handling for Silver Pipeline"""
    
    def __init__(self, spark_session: SparkSession = None):
        self.spark = spark_session or SparkSession.getActiveSession()
        self.logger = SilverPipelineLogger("pipeline_error_handler")
        self.error_records = []
    
    def create_error_log_table(self) -> DataFrame:
        """Create error log table structure"""
        error_schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("source", StringType(), True),
            StructField("lob", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("stage", StringType(), True),
            StructField("error_type", StringType(), False),
            StructField("error_message", StringType(), False),
            StructField("stack_trace", StringType(), True),
            StructField("record_count", IntegerType(), True),
            StructField("additional_context", StringType(), True)
        ])
        
        return self.spark.createDataFrame([], error_schema)
    
    def log_error(self, error_type: str, error_message: str, source: str = None,
                  lob: str = None, domain: str = None, stage: str = None,
                  exception: Exception = None, additional_context: Dict[str, Any] = None):
        """Log error with full context"""
        error_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(error_message) % 10000}"
        
        error_record = {
            'error_id': error_id,
            'timestamp': datetime.now(),
            'source': source,
            'lob': lob,
            'domain': domain,
            'stage': stage,
            'error_type': error_type,
            'error_message': error_message,
            'stack_trace': traceback.format_exc() if exception else None,
            'record_count': None,
            'additional_context': json.dumps(additional_context) if additional_context else None
        }
        
        self.error_records.append(error_record)
        
        # Log to pipeline logger
        self.logger.log_pipeline_event(
            event_type='ERROR',
            source=source or 'unknown',
            lob=lob or 'unknown', 
            domain=domain or 'unknown',
            stage=stage or 'unknown',
            message=error_message,
            additional_data={'error_id': error_id, 'error_type': error_type}
        )
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of errors encountered"""
        if not self.error_records:
            return {'total_errors': 0, 'error_types': {}, 'latest_errors': []}
        
        error_types = {}
        for error in self.error_records:
            error_type = error['error_type']
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        latest_errors = sorted(self.error_records, key=lambda x: x['timestamp'], reverse=True)[:5]
        
        return {
            'total_errors': len(self.error_records),
            'error_types': error_types,
            'latest_errors': [
                {
                    'error_id': error['error_id'],
                    'timestamp': error['timestamp'].isoformat(),
                    'error_type': error['error_type'],
                    'error_message': error['error_message'],
                    'context': f"{error['source']}/{error['lob']}/{error['domain']}/{error['stage']}"
                }
                for error in latest_errors
            ]
        }
    
    def clear_errors(self):
        """Clear accumulated error records"""
        self.error_records = []


def pipeline_error_handler(error_handler: PipelineErrorHandler, 
                         source: str = None, lob: str = None, 
                         domain: str = None, stage: str = None):
    """Decorator for pipeline functions to handle errors consistently"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                start_time = datetime.now()
                result = func(*args, **kwargs)
                end_time = datetime.now()
                
                # Log successful execution
                processing_time = (end_time - start_time).total_seconds()
                
                # Try to get record count if result is a DataFrame
                record_count = None
                if hasattr(result, 'count'):
                    try:
                        record_count = result.count()
                    except:
                        record_count = None
                
                error_handler.logger.log_performance_metrics(
                    source=source or 'unknown',
                    lob=lob or 'unknown',
                    domain=domain or 'unknown', 
                    stage=stage or func.__name__,
                    record_count=record_count or 0,
                    processing_time_seconds=processing_time
                )
                
                return result
                
            except Exception as e:
                error_handler.log_error(
                    error_type=type(e).__name__,
                    error_message=str(e),
                    source=source,
                    lob=lob,
                    domain=domain,
                    stage=stage or func.__name__,
                    exception=e,
                    additional_context={'function': func.__name__}
                )
                
                # Re-raise the exception to maintain normal error flow
                raise
        
        return wrapper
    return decorator


class CircuitBreaker:
    """Circuit breaker pattern for handling failures gracefully"""
    
    def __init__(self, failure_threshold: int = 5, timeout_seconds: int = 300):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        """Call function with circuit breaker protection"""
        if self.state == 'OPEN':
            if self._should_attempt_reset():
                self.state = 'HALF_OPEN'
            else:
                raise Exception(f"Circuit breaker is OPEN. Last failure: {self.last_failure_time}")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return True
        
        return (datetime.now() - self.last_failure_time).seconds > self.timeout_seconds
    
    def _on_success(self):
        """Handle successful execution"""
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def _on_failure(self):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'


# Global error handler instance
global_error_handler = PipelineErrorHandler()
global_logger = SilverPipelineLogger("silver_pipeline")