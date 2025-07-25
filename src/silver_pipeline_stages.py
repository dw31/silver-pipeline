"""
Silver Pipeline Stages Library
Functions corresponding to methods from SilverPipelineStages class for notebook execution
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from typing import Dict, List, Any
import logging

from utils.config_loader import PipelineConfig

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get active Spark session
spark = SparkSession.getActiveSession()


def get_schema_rules(config: PipelineConfig, source: str, lob: str, domain: str) -> List[Dict[str, Any]]:
    """
    Fetch schema rules for given source/lob/domain combination
    
    Args:
        config: Pipeline configuration object
        source: Source identifier
        lob: Line of business
        domain: Domain (pharmacy, medical, member)
        
    Returns:
        List of schema rule dictionaries
    """
    query = f"""
    SELECT field_name, data_type, is_required
    FROM {config.schema_rules_table}
    WHERE source = '{source}' 
      AND lob = '{lob}' 
      AND domain = '{domain}'
    ORDER BY field_name
    """
    
    result = spark.sql(query)
    return [row.asDict() for row in result.collect()]


def get_transform_rules(config: PipelineConfig, source: str, lob: str, domain: str) -> List[Dict[str, Any]]:
    """
    Fetch transformation rules for given source/lob/domain combination
    
    Args:
        config: Pipeline configuration object
        source: Source identifier
        lob: Line of business
        domain: Domain (pharmacy, medical, member)
        
    Returns:
        List of transform rule dictionaries sorted by priority
    """
    query = f"""
    SELECT field_name, transform, transform_type, priority
    FROM {config.transform_rules_table}
    WHERE source = '{source}' 
      AND lob = '{lob}' 
      AND domain = '{domain}'
    ORDER BY priority ASC, field_name ASC
    """
    
    result = spark.sql(query)
    return [row.asDict() for row in result.collect()]


def validate_bronze_table_exists(table_name: str) -> bool:
    """Check if Bronze table exists"""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        logger.warning(f"Bronze table {table_name} does not exist")
        return False


def apply_schema_selection(df: DataFrame, schema_rules: List[Dict[str, Any]]) -> DataFrame:
    """
    Apply schema selection rules to create Silver_A
    
    Args:
        df: Input Bronze DataFrame
        schema_rules: List of schema rule dictionaries
        
    Returns:
        DataFrame with selected fields
    """
    if not schema_rules:
        logger.warning("No schema rules found, returning original DataFrame")
        return df
    
    # Get field names to select
    fields_to_select = [rule['field_name'] for rule in schema_rules]
    
    # Check which fields exist in the DataFrame
    available_fields = df.columns
    valid_fields = [field for field in fields_to_select if field in available_fields]
    missing_fields = [field for field in fields_to_select if field not in available_fields]
    
    if missing_fields:
        logger.warning(f"Missing fields in Bronze table: {missing_fields}")
    
    if not valid_fields:
        raise ValueError("No valid fields found for schema selection")
    
    # Select valid fields
    return df.select(*valid_fields)


def apply_transformations(df: DataFrame, transform_rules: List[Dict[str, Any]]) -> DataFrame:
    """
    Apply transformation rules to create Silver_B
    
    Args:
        df: Input Silver_A DataFrame
        transform_rules: List of transform rule dictionaries
        
    Returns:
        DataFrame with transformations applied
    """
    if not transform_rules:
        logger.info("No transformation rules found, returning original DataFrame")
        return df
    
    result_df = df
    
    # Group transforms by field name to handle multiple transforms per field
    field_transforms = {}
    for rule in transform_rules:
        field_name = rule['field_name']
        if field_name not in field_transforms:
            field_transforms[field_name] = []
        field_transforms[field_name].append(rule)
    
    # Apply transformations
    for field_name, transforms in field_transforms.items():
        if field_name in result_df.columns:
            # Apply transforms in priority order (already sorted)
            for transform in transforms:
                transform_expr = transform['transform']
                try:
                    # Replace field references in transform expression
                    transformed_expr = transform_expr.replace(field_name, f"`{field_name}`")
                    result_df = result_df.withColumn(field_name, F.expr(transformed_expr))
                    logger.debug(f"Applied transform to {field_name}: {transform_expr}")
                except Exception as e:
                    logger.error(f"Failed to apply transform to {field_name}: {transform_expr}, Error: {e}")
                    # Continue with other transforms
        else:
            logger.warning(f"Field {field_name} not found in DataFrame for transformation")
    
    return result_df


def validate_required_fields(df: DataFrame, config: PipelineConfig, source: str, lob: str, domain: str) -> DataFrame:
    """Validate that all required fields are present and not null"""
    if not config.validation_enabled:
        return df
    
    schema_rules = get_schema_rules(config, source, lob, domain)
    required_fields = [rule['field_name'] for rule in schema_rules if rule['is_required']]
    
    for field in required_fields:
        if field in df.columns:
            df = df.filter(F.col(field).isNotNull())
    
    return df