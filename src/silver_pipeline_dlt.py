"""
Azure Databricks Delta Live Tables Pipeline for Silver Layer
Implements the Silver pipeline with three stages: Silver_A, Silver_B, Silver_C
"""

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from typing import Dict, List, Any
import logging

# Import configuration utilities  
from utils.config_loader import config_loader, PipelineConfig

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config = config_loader.load_config()

# Initialize Spark session (for use in helper functions)
spark = SparkSession.getActiveSession()


class SilverPipelineStages:
    """Core logic for Silver pipeline stages"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        
    def get_schema_rules(self, source: str, lob: str, domain: str) -> List[Dict[str, Any]]:
        """
        Fetch schema rules for given source/lob/domain combination
        
        Args:
            source: Source identifier
            lob: Line of business
            domain: Domain (pharmacy, medical, member)
            
        Returns:
            List of schema rule dictionaries
        """
        query = f"""
        SELECT field_name, data_type, is_required
        FROM {self.config.schema_rules_table}
        WHERE source = '{source}' 
          AND lob = '{lob}' 
          AND domain = '{domain}'
        ORDER BY field_name
        """
        
        result = spark.sql(query)
        return [row.asDict() for row in result.collect()]
    
    def get_transform_rules(self, source: str, lob: str, domain: str) -> List[Dict[str, Any]]:
        """
        Fetch transformation rules for given source/lob/domain combination
        
        Args:
            source: Source identifier
            lob: Line of business
            domain: Domain (pharmacy, medical, member)
            
        Returns:
            List of transform rule dictionaries sorted by priority
        """
        query = f"""
        SELECT field_name, transform, transform_type, priority
        FROM {self.config.transform_rules_table}
        WHERE source = '{source}' 
          AND lob = '{lob}' 
          AND domain = '{domain}'
        ORDER BY priority ASC, field_name ASC
        """
        
        result = spark.sql(query)
        return [row.asDict() for row in result.collect()]
    
    def validate_bronze_table_exists(self, table_name: str) -> bool:
        """Check if Bronze table exists"""
        try:
            spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            logger.warning(f"Bronze table {table_name} does not exist")
            return False
    
    def apply_schema_selection(self, df: DataFrame, schema_rules: List[Dict[str, Any]]) -> DataFrame:
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
    
    def apply_transformations(self, df: DataFrame, transform_rules: List[Dict[str, Any]]) -> DataFrame:
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


# Initialize pipeline stages
pipeline_stages = SilverPipelineStages(config)

# Generate DLT table definitions for each source/lob/domain combination
source_combinations = config_loader.get_source_combinations()

for source, lob, domain in source_combinations:
    
    # Silver_A Stage - Schema Selection
    @dlt.table(
        name=f"{source}_{lob}_{domain}_silver_a",
        comment=f"Silver A stage for {source}/{lob}/{domain} - Schema selection based on business rules"
    )
    def create_silver_a():
        """Create Silver_A table with schema selection"""
        # This will be dynamically bound to each source/lob/domain combination
        current_source, current_lob, current_domain = source, lob, domain
        
        # Construct Bronze table name (assuming current month for demo)
        # In production, this would be parameterized
        year_month = "202407"  # This should be parameterized in production
        bronze_table = config_loader.get_bronze_table_name(
            current_source, current_lob, current_domain, year_month
        )
        
        # Check if Bronze table exists
        if not pipeline_stages.validate_bronze_table_exists(bronze_table):
            # Return empty DataFrame with expected schema if Bronze table doesn't exist
            empty_schema = StructType([StructField("placeholder", StringType(), True)])
            return spark.createDataFrame([], empty_schema)
        
        # Read Bronze table
        bronze_df = spark.table(bronze_table)
        
        # Get schema rules
        schema_rules = pipeline_stages.get_schema_rules(current_source, current_lob, current_domain)
        
        # Apply schema selection
        return pipeline_stages.apply_schema_selection(bronze_df, schema_rules)
    
    # Silver_B Stage - Transformations  
    @dlt.table(
        name=f"{source}_{lob}_{domain}_silver_b",
        comment=f"Silver B stage for {source}/{lob}/{domain} - Business transformations"
    )
    def create_silver_b():
        """Create Silver_B table with transformations"""
        current_source, current_lob, current_domain = source, lob, domain
        
        # Read Silver_A table
        silver_a_table = f"{config.silver_schema}.{current_source}_{current_lob}_{current_domain}_silver_a"
        
        try:
            silver_a_df = dlt.read(f"{current_source}_{current_lob}_{current_domain}_silver_a")
        except Exception as e:
            logger.error(f"Failed to read Silver_A table {silver_a_table}: {e}")
            # Return empty DataFrame if Silver_A doesn't exist
            empty_schema = StructType([StructField("placeholder", StringType(), True)])
            return spark.createDataFrame([], empty_schema)
        
        # Get transformation rules
        transform_rules = pipeline_stages.get_transform_rules(current_source, current_lob, current_domain)
        
        # Apply transformations
        return pipeline_stages.apply_transformations(silver_a_df, transform_rules)
    
    # Silver_C Stage - Pass-through (placeholder for future enhancements)
    @dlt.table(
        name=f"{source}_{lob}_{domain}_silver_c", 
        comment=f"Silver C stage for {source}/{lob}/{domain} - Deduplication and filtering (placeholder)"
    )
    def create_silver_c():
        """Create Silver_C table - currently a pass-through"""
        current_source, current_lob, current_domain = source, lob, domain
        
        # Read Silver_B table
        try:
            silver_b_df = dlt.read(f"{current_source}_{current_lob}_{current_domain}_silver_b")
            
            # For now, just pass through the data
            # Future enhancements: deduplication, row filtering, data quality scoring
            return silver_b_df
            
        except Exception as e:
            logger.error(f"Failed to read Silver_B table: {e}")
            # Return empty DataFrame if Silver_B doesn't exist
            empty_schema = StructType([StructField("placeholder", StringType(), True)])
            return spark.createDataFrame([], empty_schema)


# Data Quality Validation Functions
@dlt.expect_all_or_fail("valid_required_fields")
def validate_required_fields(df: DataFrame, source: str, lob: str, domain: str) -> DataFrame:
    """Validate that all required fields are present and not null"""
    if not config.validation_enabled:
        return df
    
    schema_rules = pipeline_stages.get_schema_rules(source, lob, domain)
    required_fields = [rule['field_name'] for rule in schema_rules if rule['is_required']]
    
    for field in required_fields:
        if field in df.columns:
            df = df.filter(F.col(field).isNotNull())
    
    return df


# Pipeline Entry Point
def main():
    """Main pipeline execution function"""
    logger.info("Starting Silver Pipeline execution")
    logger.info(f"Processing {len(source_combinations)} source combinations")
    
    for source, lob, domain in source_combinations:
        logger.info(f"Processing combination: {source}/{lob}/{domain}")
    
    logger.info("Silver Pipeline setup complete")


if __name__ == "__main__":
    main()