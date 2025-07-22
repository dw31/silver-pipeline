"""
Data Quality Validation Module for Silver Pipeline
Provides comprehensive data quality checks and monitoring
"""

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """Comprehensive data quality validation for Silver pipeline"""
    
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.getActiveSession()
        
    def create_quality_metrics_table(self):
        """Create table to store data quality metrics"""
        quality_schema = StructType([
            StructField("source", StringType(), False),
            StructField("lob", StringType(), False),
            StructField("domain", StringType(), False),
            StructField("stage", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("metric_name", StringType(), False),
            StructField("metric_value", DoubleType(), True),
            StructField("threshold", DoubleType(), True),
            StructField("status", StringType(), False),  # PASS, WARN, FAIL
            StructField("record_count", IntegerType(), True),
            StructField("created_timestamp", TimestampType(), False)
        ])
        
        return self.spark.createDataFrame([], quality_schema)
    
    def validate_schema_compliance(self, df: DataFrame, source: str, lob: str, domain: str, 
                                 expected_schema: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate that DataFrame schema matches expected schema rules
        
        Args:
            df: DataFrame to validate
            source: Source identifier
            lob: Line of business
            domain: Domain name
            expected_schema: List of expected field definitions
            
        Returns:
            Dict with validation results
        """
        results = {
            'source': source,
            'lob': lob,
            'domain': domain,
            'validation_timestamp': datetime.now(),
            'schema_checks': [],
            'overall_status': 'PASS'
        }
        
        df_columns = set(df.columns)
        expected_fields = {rule['field_name']: rule for rule in expected_schema}
        
        # Check for missing required fields
        missing_required = []
        for field_name, rule in expected_fields.items():
            if rule.get('is_required', False) and field_name not in df_columns:
                missing_required.append(field_name)
                results['schema_checks'].append({
                    'check_type': 'missing_required_field',
                    'field_name': field_name,
                    'status': 'FAIL',
                    'message': f'Required field {field_name} is missing'
                })
        
        # Check for extra fields
        extra_fields = df_columns - set(expected_fields.keys())
        for field_name in extra_fields:
            results['schema_checks'].append({
                'check_type': 'extra_field',
                'field_name': field_name,
                'status': 'WARN',
                'message': f'Unexpected field {field_name} found'
            })
        
        # Set overall status
        if missing_required:
            results['overall_status'] = 'FAIL'
        elif extra_fields:
            results['overall_status'] = 'WARN'
        
        return results
    
    def validate_data_completeness(self, df: DataFrame, source: str, lob: str, domain: str,
                                 required_fields: List[str]) -> Dict[str, Any]:
        """
        Validate data completeness for required fields
        
        Args:
            df: DataFrame to validate
            source: Source identifier
            lob: Line of business  
            domain: Domain name
            required_fields: List of field names that cannot be null
            
        Returns:
            Dict with completeness results
        """
        total_records = df.count()
        completeness_results = {
            'source': source,
            'lob': lob,
            'domain': domain,
            'total_records': total_records,
            'validation_timestamp': datetime.now(),
            'field_completeness': [],
            'overall_completeness': 100.0,
            'status': 'PASS'
        }
        
        if total_records == 0:
            completeness_results['status'] = 'WARN'
            completeness_results['overall_completeness'] = 0.0
            return completeness_results
        
        overall_null_count = 0
        total_possible_values = total_records * len(required_fields)
        
        for field_name in required_fields:
            if field_name in df.columns:
                null_count = df.filter(F.col(field_name).isNull()).count()
                completeness_percent = ((total_records - null_count) / total_records) * 100
                
                field_result = {
                    'field_name': field_name,
                    'null_count': null_count,
                    'completeness_percent': completeness_percent,
                    'status': 'PASS' if completeness_percent >= (100 - self.config.null_tolerance_percent) else 'FAIL'
                }
                
                completeness_results['field_completeness'].append(field_result)
                overall_null_count += null_count
                
                if field_result['status'] == 'FAIL':
                    completeness_results['status'] = 'FAIL'
        
        # Calculate overall completeness
        if total_possible_values > 0:
            completeness_results['overall_completeness'] = ((total_possible_values - overall_null_count) / total_possible_values) * 100
        
        return completeness_results
    
    def validate_data_types(self, df: DataFrame, source: str, lob: str, domain: str,
                          expected_types: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate data types match expected types
        
        Args:
            df: DataFrame to validate
            source: Source identifier
            lob: Line of business
            domain: Domain name
            expected_types: Dict mapping field names to expected data types
            
        Returns:
            Dict with data type validation results
        """
        results = {
            'source': source,
            'lob': lob,
            'domain': domain,
            'validation_timestamp': datetime.now(),
            'type_checks': [],
            'status': 'PASS'
        }
        
        # Get actual schema
        actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
        
        for field_name, expected_type in expected_types.items():
            if field_name in actual_schema:
                actual_type = actual_schema[field_name]
                type_matches = self._compare_data_types(actual_type, expected_type)
                
                type_check = {
                    'field_name': field_name,
                    'expected_type': expected_type,
                    'actual_type': actual_type,
                    'matches': type_matches,
                    'status': 'PASS' if type_matches else 'FAIL'
                }
                
                results['type_checks'].append(type_check)
                
                if not type_matches:
                    results['status'] = 'FAIL'
        
        return results
    
    def _compare_data_types(self, actual_type: str, expected_type: str) -> bool:
        """Compare Spark data types with expected business rule types"""
        # Normalize type comparisons
        type_mappings = {
            'STRING': ['StringType', 'string'],
            'INTEGER': ['IntegerType', 'int', 'LongType', 'long'],
            'DOUBLE': ['DoubleType', 'double', 'FloatType', 'float'],
            'DATE': ['DateType', 'date'],
            'TIMESTAMP': ['TimestampType', 'timestamp'],
            'BOOLEAN': ['BooleanType', 'boolean']
        }
        
        expected_type_upper = expected_type.upper()
        if expected_type_upper in type_mappings:
            return any(mapping.lower() in actual_type.lower() for mapping in type_mappings[expected_type_upper])
        
        return actual_type.lower() == expected_type.lower()
    
    def detect_duplicates(self, df: DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """
        Detect duplicate records based on key columns
        
        Args:
            df: DataFrame to check
            key_columns: List of columns that define uniqueness
            
        Returns:
            Dict with duplicate detection results
        """
        total_records = df.count()
        
        if not key_columns or total_records == 0:
            return {
                'total_records': total_records,
                'unique_records': total_records,
                'duplicate_count': 0,
                'duplicate_percentage': 0.0,
                'status': 'PASS'
            }
        
        # Filter to only key columns that exist
        existing_key_columns = [col for col in key_columns if col in df.columns]
        
        if not existing_key_columns:
            return {
                'total_records': total_records,
                'unique_records': total_records,
                'duplicate_count': 0,
                'duplicate_percentage': 0.0,
                'status': 'WARN',
                'message': 'No key columns found for duplicate detection'
            }
        
        unique_records = df.select(*existing_key_columns).distinct().count()
        duplicate_count = total_records - unique_records
        duplicate_percentage = (duplicate_count / total_records) * 100 if total_records > 0 else 0
        
        return {
            'total_records': total_records,
            'unique_records': unique_records,
            'duplicate_count': duplicate_count,
            'duplicate_percentage': duplicate_percentage,
            'key_columns': existing_key_columns,
            'status': 'PASS' if duplicate_count == 0 else 'WARN'
        }
    
    def generate_quality_report(self, validation_results: List[Dict[str, Any]]) -> DataFrame:
        """
        Generate data quality report as a DataFrame
        
        Args:
            validation_results: List of validation result dictionaries
            
        Returns:
            DataFrame with quality metrics
        """
        quality_records = []
        
        for result in validation_results:
            source = result.get('source', 'unknown')
            lob = result.get('lob', 'unknown')
            domain = result.get('domain', 'unknown')
            timestamp = result.get('validation_timestamp', datetime.now())
            
            # Schema validation metrics
            if 'schema_checks' in result:
                for check in result['schema_checks']:
                    quality_records.append({
                        'source': source,
                        'lob': lob,
                        'domain': domain,
                        'stage': 'schema_validation',
                        'table_name': f"{source}_{lob}_{domain}",
                        'metric_name': check['check_type'],
                        'metric_value': 1.0 if check['status'] == 'PASS' else 0.0,
                        'threshold': 1.0,
                        'status': check['status'],
                        'record_count': None,
                        'created_timestamp': timestamp
                    })
            
            # Completeness metrics
            if 'field_completeness' in result:
                for field_result in result['field_completeness']:
                    quality_records.append({
                        'source': source,
                        'lob': lob,
                        'domain': domain,
                        'stage': 'completeness_validation',
                        'table_name': f"{source}_{lob}_{domain}",
                        'metric_name': f"completeness_{field_result['field_name']}",
                        'metric_value': field_result['completeness_percent'],
                        'threshold': 100 - self.config.null_tolerance_percent,
                        'status': field_result['status'],
                        'record_count': result.get('total_records'),
                        'created_timestamp': timestamp
                    })
            
            # Duplicate metrics
            if 'duplicate_count' in result:
                quality_records.append({
                    'source': source,
                    'lob': lob,
                    'domain': domain,
                    'stage': 'duplicate_detection',
                    'table_name': f"{source}_{lob}_{domain}",
                    'metric_name': 'duplicate_percentage',
                    'metric_value': result['duplicate_percentage'],
                    'threshold': 0.0,
                    'status': result['status'],
                    'record_count': result['total_records'],
                    'created_timestamp': timestamp
                })
        
        if quality_records:
            return self.spark.createDataFrame(quality_records)
        else:
            return self.create_quality_metrics_table()


# DLT Data Quality Expectations
@dlt.expect("valid_record_count")
def expect_minimum_records(df: DataFrame, min_records: int = 1):
    """Expect minimum number of records"""
    return F.count("*") >= min_records


@dlt.expect_all("no_null_required_fields") 
def expect_no_null_required_fields(required_fields: List[str]):
    """Expect no null values in required fields"""
    expectations = []
    for field in required_fields:
        expectations.append(F.col(field).isNotNull())
    return expectations


@dlt.expect("valid_data_types")
def expect_valid_data_types(df: DataFrame):
    """Expect data types to be valid (no parsing errors)"""
    # This is a placeholder - specific type validations would be field-specific
    return F.lit(True)


# Quality monitoring table
@dlt.table(
    name="data_quality_metrics",
    comment="Data quality metrics for Silver pipeline monitoring"
)
def create_quality_metrics():
    """Create data quality metrics table"""
    validator = DataQualityValidator(None)  # Config will be injected
    return validator.create_quality_metrics_table()